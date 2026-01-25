import os
import json
import datetime
import requests
import pandas as pd

API_KEY = os.environ.get("NASDAQ_API_KEY")
if not API_KEY:
    raise SystemExit("Missing NASDAQ_API_KEY")

# Example: 0.045 means 4.5%
TREASURY_10Y = float(os.environ.get("TREASURY_10Y", "0.045"))

BASE = "https://data.nasdaq.com/api/v3/datatables"


def _raise_with_context(resp: requests.Response) -> None:
    if resp.ok:
        return
    snippet = (resp.text or "")[:1200]
    print(f"HTTP {resp.status_code} error. Response snippet:\n{snippet}\n")
    resp.raise_for_status()


def get_json(url: str, params: dict, timeout: int = 180) -> dict:
    resp = requests.get(url, params=params, timeout=timeout)
    _raise_with_context(resp)
    return resp.json()


def fetch_datatable_all_rows(code: str, params: dict, timeout: int = 180, max_pages: int = 500) -> pd.DataFrame:
    """
    Fetch rows from a Nasdaq Data Link datatable using JSON pagination (cursor_id).
    Returns a DataFrame with lowercase column names.
    """
    url = f"{BASE}/{code}.json"
    all_rows = []
    columns = None
    cursor = None

    for _ in range(max_pages):
        p = dict(params)
        p["api_key"] = API_KEY
        if cursor:
            p["qopts.cursor_id"] = cursor

        j = get_json(url, p, timeout=timeout)
        dt = j.get("datatable", {})

        if columns is None:
            columns = [c["name"] for c in dt.get("columns", [])]
            if not columns:
                raise SystemExit(f"No columns returned for {code}. Check access/params.")

        all_rows.extend(dt.get("data", []))

        cursor = j.get("meta", {}).get("next_cursor_id")
        if not cursor:
            break

    return pd.DataFrame(all_rows, columns=[c.lower() for c in columns])


def get_latest_sep_date() -> str:
    """
    IMPORTANT:
    Datatables sort syntax: use -date for descending (NOT 'date desc').
    """
    url = f"{BASE}/SHARADAR/SEP.json"
    params = {
        "api_key": API_KEY,
        "qopts.columns": "date",
        "qopts.sort": "-date",
        "qopts.per_page": 1,
    }
    j = get_json(url, params, timeout=120)
    cols = [c["name"] for c in j["datatable"]["columns"]]
    row = j["datatable"]["data"][0]
    d = dict(zip([c.lower() for c in cols], row))
    return str(d["date"])


def load_sep_closes(latest_date: str) -> pd.DataFrame:
    """
    Pull ALL tickers' close for a specific date.
    """
    df = fetch_datatable_all_rows(
        "SHARADAR/SEP",
        params={
            "date": latest_date,
            "qopts.columns": "ticker,close",
            "qopts.per_page": 10000,
        },
        timeout=180,
        max_pages=300,
    )
    if df.empty:
        raise SystemExit("SEP returned 0 rows. Your SEP access may be limited.")
    if "ticker" not in df.columns or "close" not in df.columns:
        raise SystemExit(f"SEP missing expected columns. Got: {list(df.columns)}")

    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["ticker", "close"]).drop_duplicates(subset=["ticker"])
    return df


def load_sf1_art_fundamentals() -> pd.DataFrame:
    """
    Pull ART fundamentals for:
    epsusd (for PE), bvps (for PB), dps (for yield), debtnc & equity (LT debt/equity)
    """
    df = fetch_datatable_all_rows(
        "SHARADAR/SF1",
        params={
            "dimension": "ART",
            "qopts.columns": "ticker,dimension,calendardate,lastupdated,epsusd,bvps,dps,debtnc,equity",
            "qopts.per_page": 10000,
        },
        timeout=180,
        max_pages=500,
    )
    needed = {"ticker", "calendardate", "lastupdated", "epsusd", "bvps", "dps", "debtnc", "equity"}
    missing = needed - set(df.columns)
    if missing:
        raise SystemExit(f"SF1 missing required columns: {sorted(missing)}")

    # pick latest ART row per ticker
    df["calendardate"] = pd.to_datetime(df["calendardate"], errors="coerce")
    df["lastupdated"] = pd.to_datetime(df["lastupdated"], errors="coerce")
    df = df.sort_values(["ticker", "calendardate", "lastupdated"])
    df = df.groupby("ticker", as_index=False).tail(1)

    for c in ["epsusd", "bvps", "dps", "debtnc", "equity"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    return df


def main() -> None:
    latest_price_date = get_latest_sep_date()
    closes = load_sep_closes(latest_price_date)

    sf1 = load_sf1_art_fundamentals()

    # Merge current close with latest fundamentals per ticker
    df = closes.merge(sf1, on="ticker", how="inner")

    # Compute CURRENT ratios using current close
    df["pe"] = df["close"] / df["epsusd"]
    df["pb"] = df["close"] / df["bvps"]
    df["div_yield"] = df["dps"] / df["close"]
    df["lt_debt_equity"] = df["debtnc"] / df["equity"]

    for c in ["pe", "pb", "div_yield", "lt_debt_equity"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # Apply Wilson's Algorithm (your definition)
    df["passes"] = (
        df["div_yield"].notna() & (df["div_yield"] >= TREASURY_10Y) &
        df["pe"].notna() & (df["pe"] <= 13) &
        df["pb"].notna() & (df["pb"] <= 1) &
        df["lt_debt_equity"].notna() & (df["lt_debt_equity"] <= 1)
    )

    winners = df[df["passes"]].copy().sort_values("div_yield", ascending=False)

    pass_list = []
    for _, r in winners.iterrows():
        pass_list.append({
            "ticker": r["ticker"],
            "price": round(float(r["close"]), 2),
            "pe": round(float(r["pe"]), 4) if pd.notna(r["pe"]) else None,
            "pb": round(float(r["pb"]), 4) if pd.notna(r["pb"]) else None,
            "divYield": round(float(r["div_yield"]), 6) if pd.notna(r["div_yield"]) else None,
            "ltDebtEquity": round(float(r["lt_debt_equity"]), 4) if pd.notna(r["lt_debt_equity"]) else None,
            "priceDate": str(latest_price_date),
        })

    out = {
        "runDate": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "asOfPriceDate": str(latest_price_date),
        "treasuryYield10y": TREASURY_10Y,
        "passCount": len(pass_list),
        "pass": pass_list,
    }

    with open("passlist.json", "w") as f:
        json.dump(out, f, indent=2)

    print(f"Wrote passlist.json with {len(pass_list)} PASS tickers (as of {latest_price_date})")


if __name__ == "__main__":
    main()
