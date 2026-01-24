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
    snippet = (resp.text or "")[:1200]
    print(f"HTTP {resp.status_code} error. Response snippet:\n{snippet}\n")
    resp.raise_for_status()


def get_json(url: str, params: dict, timeout: int = 120) -> dict:
    resp = requests.get(url, params=params, timeout=timeout)
    _raise_with_context(resp)
    return resp.json()


def fetch_datatable_all_rows(code: str, params: dict, timeout: int = 120, max_pages: int = 200) -> pd.DataFrame:
    """
    Fetch rows from a Nasdaq Data Link datatable using JSON pagination.
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

        data = dt.get("data", [])
        all_rows.extend(data)

        cursor = j.get("meta", {}).get("next_cursor_id")
        if not cursor:
            break

    df = pd.DataFrame(all_rows, columns=[c.lower() for c in columns])
    return df


def latest_trading_day_from_sep() -> str:
    """
    Determine latest trading date from SEP using a small sample and max(date).
    """
    df = fetch_datatable_all_rows(
        "SHARADAR/SEP",
        params={
            "qopts.columns": "date",
            "qopts.per_page": 1000,
        },
        max_pages=5,
        timeout=120,
    )
    if df.empty or "date" not in df.columns:
        raise SystemExit("Could not determine latest date from SEP.")
    return str(df["date"].max())


def load_sep_closes(latest_date: str) -> pd.DataFrame:
    """
    Get ticker + close for all tickers on latest_date from SEP via JSON paging.
    """
    df = fetch_datatable_all_rows(
        "SHARADAR/SEP",
        params={
            "date": latest_date,
            "qopts.columns": "ticker,close",
            "qopts.per_page": 10000,
        },
        max_pages=50,
        timeout=180,
    )
    if "ticker" not in df.columns or "close" not in df.columns:
        raise SystemExit(f"SEP missing expected columns. Got: {list(df.columns)}")

    df = df.dropna(subset=["ticker", "close"]).drop_duplicates(subset=["ticker"])
    return df[["ticker", "close"]]


def probe_sf1_columns_and_exit() -> None:
    """
    PROBE MODE:
    Print your SF1 schema column names (so we can map dividend + debt correctly),
    then exit intentionally.
    """
    url = f"{BASE}/SHARADAR/SF1.json"
    params = {
        "api_key": API_KEY,
        "qopts.per_page": 1,
    }
    j = get_json(url, params, timeout=120)
    cols = [c["name"] for c in j["datatable"]["columns"]]
    print("\n========================")
    print("SF1 columns:", cols)
    print("========================\n")
    raise SystemExit("Printed SF1 columns. Paste the SF1 columns line back into chat.")


def main() -> None:
    # Confirm SEP works (prices)
    latest_date = latest_trading_day_from_sep()
    closes = load_sep_closes(latest_date)
    print(f"SEP OK. Latest date = {latest_date}. Closes rows = {len(closes)}")

    # Now print SF1 schema and stop (so we can map fields)
    probe_sf1_columns_and_exit()


if __name__ == "__main__":
    main()
