import os
import json
import datetime
import requests
import pandas as pd

API_KEY = os.environ.get("NASDAQ_API_KEY")
if not API_KEY:
    raise SystemExit("Missing NASDAQ_API_KEY")

# Set in GitHub Actions Variables: TREASURY_10Y (example: 0.045 = 4.5%)
TREASURY_10Y = float(os.environ.get("TREASURY_10Y", "0.045"))

BASE = "https://data.nasdaq.com/api/v3/datatables"


def _raise_with_context(resp: requests.Response) -> None:
    """Print a helpful snippet before raising HTTP errors (shows up in Actions logs)."""
    if resp.ok:
        return
    snippet = (resp.text or "")[:800]
    print(f"HTTP {resp.status_code} error. Response snippet:\n{snippet}\n")
    resp.raise_for_status()


def get_json(url: str, params: dict, timeout: int = 120) -> dict:
    resp = requests.get(url, params=params, timeout=timeout)
    _raise_with_context(resp)
    return resp.json()


def download_csv(url: str, params: dict, timeout: int = 300) -> pd.DataFrame:
    resp = requests.get(url, params=params, timeout=timeout)
    _raise_with_context(resp)
    return pd.read_csv(pd.io.common.BytesIO(resp.content))


def latest_trading_day_from_sep() -> str:
    """
    IMPORTANT FIX:
    Do NOT use qopts.sort on this endpoint. It can return 422.
    Instead, fetch a sample and take max(date) locally.
    """
    url = f"{BASE}/SHARADAR/SEP.json"
    params = {
        "api_key": API_KEY,
        "qopts.columns": "date",
        "qopts.per_page": 1000,
    }
    j = get_json(url, params)

    cols = [c["name"] for c in j["datatable"]["columns"]]
    if "date" not in cols:
        raise SystemExit(f"SEP JSON did not include 'date'. Columns: {cols}")

    dates = []
    for row in j["datatable"]["data"]:
        d = dict(zip(cols, row)).get("date")
        if d:
            dates.append(d)

    if not dates:
        raise SystemExit("Could not determine latest date from SEP sample (no dates returned).")

    return max(dates)


def load_sep_closes(latest_date: str) -> pd.DataFrame:
    """
    Download close prices for all tickers on the latest trading day.
    """
    url = f"{BASE}/SHARADAR/SEP.csv"
    params = {
        "api_key": API_KEY,
        "date": latest_date,
        "qopts.columns": "ticker,close",
        "qopts.export": "true",
    }
    df = download_csv(url, params)
    df.columns = [c.lower() for c in df.columns]
    df = df.dropna(subset=["ticker", "close"]).drop_duplicates(subset=["ticker"])
    return df


def load_sf1_latest() -> pd.DataFrame:
    """
    Download fundamentals for Wilson's Algorithm.
    - ART: epsusd, dpsttm (TTM-ish)
    - MRQ: bvps, debtlt, equity (balance sheet-ish)
    """
    url = f"{BASE}/SHARADAR/SF1.csv"
    params = {
        "api_key": API_KEY,
        "dimension": "ART,MRQ",
        "qopts.columns": "ticker,dimension,calendardate,lastupdated,epsusd,bvps,dpsttm,debtlt,equity",
        "qopts.export": "true",
    }
    df = download_csv(url, params)
    df.columns = [c.lower() for c in df.columns]
    return df


def pick_latest_by_dimension(df: pd.DataFrame, dim: str) -> pd.DataFrame:
    """
    For each ticker, pick the latest row for the dimension by calendardate then lastupdated.
    """
    sub = df[df["dimension"] == dim].copy()
    sub["calendardate"] = pd.to_datetime(sub["calendardate"], errors="coerce")
    sub["lastupdated"] = pd.to_datetime(sub["lastupdated"], errors="coerce")
    sub = sub.sort_values(["ticker", "calendardate", "lastupdated"])
    return sub.groupby("ticker", as_index=False).tail(1)


def main() -> None:
    # 1) Latest trading day and closes
    latest_date = latest_trading_day_from_sep()
    closes = load_sep_closes(latest_date)

    # 2) Fundamentals snapshot
    sf1 = load_sf1_latest()

    required = {"ticker", "dimension", "calendardate", "epsusd", "bvps", "debtlt", "equity"}
    missing = required - set(sf1.columns)
    if missing:
        raise SystemExit(
            f"SF1 missing columns: {sorted(missing)}. "
            f"Your SF1 schema may differ; paste this line to me and I’ll tailor it."
        )

    art = pick_latest_by_dimension(sf1, "ART")
    mrq = pick_latest_by_dimension(sf1, "MRQ")

    # 3) Merge close + fundamentals
    df = closes.merge(art, on="ticker", how="inner")
    df = df.merge(mrq[["ticker", "bvps", "debtlt", "equity"]], on="ticker", how="left")

    # 4) Compute metrics
    df["pe"] = df["close"] / df["epsusd"]
    df["pb"] = df["close"] / df["bvps"]
    df["debt_equity"] = df["debtlt"] / df["equity"]

    if "dpsttm" in df.columns:
        df["div_yield"] = df["dpsttm"] / df["close"]
    else:
        df["div_yield"] = pd.NA

    for col in ["close", "pe", "pb", "debt_equity", "div_yield"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # 5) Apply Wilson's Algorithm
    df["passes"] = (
        df["div_yield"].notna()
        & (df["div_yield"] >= TREASURY_10Y)
        & (df["pe"] <= 13)
        & (df["pb"] <= 1)
        & (df["debt_equity"] <= 1)
    )

    winners = df[df["passes"]].copy().sort_values("div_yield", ascending=False)

    pass_list = []
    for _, r in winners.iterrows():
        pass_list.append(
            {
                "ticker": r["ticker"],
                "price": round(float(r["close"]), 2) if pd.notna(r["close"]) else None,
                "pe": round(float(r["pe"]), 4) if pd.notna(r["pe"]) else None,
                "pb": round(float(r["pb"]), 4) if pd.notna(r["pb"]) else None,
                "divYield": round(float(r["div_yield"]), 6) if pd.notna(r["div_yield"]) else None,
                "debtEquity": round(float(r["debt_equity"]), 4) if pd.notna(r["debt_equity"]) else None,
                "priceDate": str(latest_date),
            }
        )

    out = {
        "runDate": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "asOfPriceDate": str(latest_date),
        "treasuryYield10y": TREASURY_10Y,
        "pass": pass_list,
    }

    with open("passlist.json", "w") as f:
        json.dump(out, f, indent=2)

    print(f"Wrote passlist.json with {len(pass_list)} PASS tickers")


if __name__ == "__main__":
    main()
