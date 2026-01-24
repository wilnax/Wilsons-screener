mport os
import json
import datetime
import requests
import pandas as pd

API_KEY = os.environ.get("NASDAQ_API_KEY")
if not API_KEY:
    raise SystemExit("Missing NASDAQ_API_KEY")

# GitHub Actions variable TREASURY_10Y (example: 0.045 = 4.5%)
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

        data = dt.get("data", [])
        all_rows.extend(data)

        cursor = j.get("meta", {}).get("next_cursor_id")
        if not cursor:
            break

    df = pd.DataFrame(all_rows, columns=[c.lower() for c in columns])
    return df


def pick_latest_per_ticker(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pick the latest row per ticker using calendardate then lastupdated.
    """
    df = df.copy()
    df["calendardate"] = pd.to_datetime(df["calendardate"], errors="coerce")
    df["lastupdated"] = pd.to_datetime(df["lastupdated"], errors="coerce")
    df = df.sort_values(["ticker", "calendardate", "lastupdated"])
    return df.groupby("ticker", as_index=False).tail(1)


def main() -> None:
    # Pull SF1 only — your schema already provides price/pe/pb/divyield.
    # Use ART (annual trailing-style) as a single consistent dimension for ratios.
    cols = "ticker,dimension,calendardate,lastupdated,price,pe,pb,divyield,de,debt,debtnc,equity"

    sf1 = fetch_datatable_all_rows(
        "SHARADAR/SF1",
        params={
            "dimension": "ART",
            "qopts.columns": cols,
            "qopts.per_page": 10000,
        },
        timeout=180,
        max_pages=500,
    )

    needed = {"ticker", "calendardate", "price", "pe", "pb", "divyield"}
    missing = needed - set(sf1.columns)
    if missing:
        raise SystemExit(f"SF1 missing required columns: {sorted(missing)}")

    latest = pick_latest_per_ticker(sf1)

    # Numeric coercion
    for col in ["price", "pe", "pb", "divyield", "de", "debt", "debtnc", "equity"]:
        if col in latest.columns:
            latest[col] = pd.to_numeric(latest[col], errors="coerce")

    # Long-term debt proxy:
    # Prefer non-current debt (debtnc) / equity; else fall back to 'de' (debt/equity ratio),
    # else debt / equity.
    debt_equity = None
    if "debtnc" in latest.columns and "equity" in latest.columns:
        debt_equity = latest["debtnc"] / latest["equity"]
    elif "de" in latest.columns:
        debt_equity = latest["de"]
    elif "debt" in latest.columns and "equity" in latest.columns:
        debt_equity = latest["debt"] / latest["equity"]
    else:
        debt_equity = pd.Series([pd.NA] * len(latest))

    latest["debt_equity_calc"] = pd.to_numeric(debt_equity, errors="coerce")

    # Apply Wilson's Algorithm
    # - Dividend yield >= 10Y treasury
    # - PE <= 13
    # - PB <= 1
    # - Long-term debt/equity <= 1 (proxy as above)
    latest["passes"] = (
        latest["divyield"].notna()
        & (latest["divyield"] >= TREASURY_10Y)
        & latest["pe"].notna()
        & (latest["pe"] <= 13)
        & latest["pb"].notna()
        & (latest["pb"] <= 1)
        & latest["debt_equity_calc"].notna()
        & (latest["debt_equity_calc"] <= 1)
    )

    winners = latest[latest["passes"]].copy()
    winners = winners.sort_values("divyield", ascending=False)

    pass_list = []
    for _, r in winners.iterrows():
        price_date = r["calendardate"]
        pass_list.append(
            {
                "ticker": r["ticker"],
                "price": round(float(r["price"]), 2) if pd.notna(r["price"]) else None,
                "pe": round(float(r["pe"]), 4) if pd.notna(r["pe"]) else None,
                "pb": round(float(r["pb"]), 4) if pd.notna(r["pb"]) else None,
                "divYield": round(float(r["divyield"]), 6) if pd.notna(r["divyield"]) else None,
                "debtEquity": round(float(r["debt_equity_calc"]), 4) if pd.notna(r["debt_equity_calc"]) else None,
                "priceDate": str(price_date.date()) if pd.notna(price_date) else None,
            }
        )

    out = {
        "runDate": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "treasuryYield10y": TREASURY_10Y,
        "dimension": "ART",
        "pass": pass_list,
    }

    with open("passlist.json", "w") as f:
        json.dump(out, f, indent=2)

    print(f"Wrote passlist.json with {len(pass_list)} PASS tickers")


if __name__ == "__main__":
    main()
