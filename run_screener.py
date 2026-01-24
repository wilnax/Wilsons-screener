import os
import json
import datetime
import requests
import pandas as pd

API_KEY = os.environ.get("NASDAQ_API_KEY")
if not API_KEY:
    raise SystemExit("Missing NASDAQ_API_KEY")

TREASURY_10Y = float(os.environ.get("TREASURY_10Y", "0.045"))
BASE = "https://data.nasdaq.com/api/v3/datatables"


def _raise_with_context(resp: requests.Response) -> None:
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
        raise SystemExit("Could not determine latest date from SEP sample.")
    return max(dates)


def _find_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    cols = list(df.columns)
    for c in candidates:
        if c in cols:
            return c
    return None


def load_sep_closes(latest_date: str) -> pd.DataFrame:
    """
    Download close prices for all tickers on a given date.
    Make this robust to schema differences by auto-detecting columns.
    """
    url = f"{BASE}/SHARADAR/SEP.csv"
    params = {
        "api_key": API_KEY,
        "date": latest_date,
        # Ask for the expected columns, but don't assume they'll come back exactly.
        "qopts.columns": "ticker,close",
        "qopts.export": "true",
    }
    df = download_csv(url, params)
    df.columns = [c.strip().lower() for c in df.columns]

    ticker_col = _find_col(df, ["ticker", "symbol"])
    close_col = _find_col(df, ["close", "adj_close", "closeadj", "close_adj", "closeadjusted", "adjclose"])

    if ticker_col is None or close_col is None:
        print("SEP.csv columns returned:", list(df.columns))
        raise SystemExit(
            "Could not find ticker/close columns in SEP.csv. "
            "See printed columns above; we will map them."
        )

    out = df[[ticker_col, close_col]].copy()
    out = out.rename(columns={ticker_col: "ticker", close_col: "close"})
    out = out.dropna(subset=["ticker", "close"]).drop_duplicates(subset=["ticker"])
    return out


def load_sf1_latest() -> pd.DataFrame:
    url = f"{BASE}/SHARADAR/SF1.csv"
    params = {
        "api_key": API_KEY,
        "dimension": "ART,MRQ",
        "qopts.columns": "ticker,dimension,calendardate,lastupdated,epsusd,bvps,dpsttm,debtlt,equity",
        "qopts.export": "true",
    }
    df = download_csv(url, params)
    df.columns = [c.strip().lower() for c in df.columns]
    return df


def pick_latest_by_dimension(df: pd.DataFrame, dim: str) -> pd.DataFrame:
    sub = df[df["dimension"] == dim].copy()
    sub["calendardate"] = pd.to_datetime(sub["calendardate"], errors="coerce")
    sub["lastupdated"] = pd.to_datetime(sub["lastupdated"], errors="coerce")
    sub = sub.sort_values(["ticker", "calendardate", "lastupdated"])
    return sub.groupby("ticker", as_index=False).tail(1)


def main() -> None:
    latest_date = latest_trading_day_from_sep()
    closes = load_sep_closes(latest_date)

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

    df = closes.merge(art, on="ticker", how="inner")
    df = df.merge(mrq[["ticker", "bvps", "debtlt", "equity"]], on="ticker", how="left")

    df["pe"] = df["close"] / df["epsusd"]
    df["pb"] = df["close"] / df["bvps"]
    df["debt_equity"] = df["debtlt"] / df["equity"]
    df["div_yield"] = df["dpsttm"] / df["close"] if "dpsttm" in df.columns else pd.NA

    for col in ["close", "pe", "pb", "debt_equity", "div_yield"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

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
