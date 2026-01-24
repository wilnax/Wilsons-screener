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


def fetch_datatable_all_rows(code: str, params: dict, timeout: int = 120, max_pages: int = 200) -> pd.DataFrame:
    """
    Fetch all rows from a Nasdaq Data Link datatable using JSON pagination.
    Returns a pandas DataFrame with lowercased columns.
    """
    url = f"{BASE}/{code}.json"
    all_rows = []
    columns = None

    # datatables uses `next_cursor_id` for pagination when results are large
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
    )
    if "date" not in df.columns or df.empty:
        raise SystemExit("Could not determine latest date from SEP.")
    # Dates come as strings like YYYY-MM-DD
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
    # Normalize and keep only needed
    if "ticker" not in df.columns or "close" not in df.columns:
        raise SystemExit(f"SEP missing expected columns. Got: {list(df.columns)}")

    df = df.dropna(subset=["ticker", "close"]).drop_duplicates(subset=["ticker"])
    return df[["ticker", "close"]]


def load_sf1_latest() -> pd.DataFrame:
    """
    Pull SF1 fundamentals (ART + MRQ) via JSON paging.
    """
    df = fetch_datatable_all_rows(
        "SHARADAR/SF1",
        params={
            "dimension": "ART,MRQ",
            "qopts.columns": "ticker,dimension,calendardate,lastupdated,epsusd,bvps,dpsttm,debtlt,equity",
            "qopts.per_page": 10000,
        },
        max_pages=200,
        timeout=180,
    )
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
        raise SystemExit(f"SF1 missing columns: {sorted(missing)}")

    art = pick_latest_by_dimension(sf1, "ART")
    mrq = pick_latest_by_dimension(sf1, "MRQ")

    df = closes.merge(art, on="ticker", how="inner")
    df = df.merge(mrq[["ticker", "bvps", "debtlt", "equity"]], on="ticker", how="left")

    # Compute metrics
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
