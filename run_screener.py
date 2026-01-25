import os
import json
import datetime
import requests
import pandas as pd

API_KEY = os.environ.get("NASDAQ_API_KEY")
if not API_KEY:
    raise SystemExit("Missing NASDAQ_API_KEY")

# Set this in GitHub Actions Variables as a DECIMAL (4.23% = 0.0423)
TREASURY_10Y = float(os.environ.get("TREASURY_10Y", "0.0423"))

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


def get_latest_sep_date_no_sort() -> str:
    """
    Your endpoint does NOT support qopts.sort.
    Pull a sample and take max(date) locally.
    """
    df = fetch_datatable_all_rows(
        "SHARADAR/SEP",
        params={
            "qopts.columns": "date",
            "qopts.per_page": 1000,
        },
        max_pages=5,
        timeout=180,
    )
    if df.empty or "date" not in df.columns:
        raise SystemExit("Could not determine latest SEP date (no dates returned).")
    return str(df["date"].max())


def load_sep_closes(latest_date: str) -> pd.DataFrame:
    df = fetch_datatable_all_rows(
        "SHARADAR/SEP",
        params={
            "date": latest_date,
            "qopts.columns": "ticker,close",
            "qopts.per_page": 10000,
        },
        max_pages=500,
        timeout=180,
    )

    if df.empty:
        raise SystemExit("SEP returned 0 rows for that date. Your SEP access may be limited.")

    if "ticker" not in df.columns or "close" not in df.columns:
        raise SystemExit(f"SEP missing expected columns. Got: {list(df.columns)}")

    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["ticker", "close"]).drop_duplicates(subset=["ticker"])
    return df[["ticker", "close"]]


def load_sf1_latest_art() -> pd.DataFrame:
    """
    Pull fundamentals needed to compute ratios using CURRENT price.
    Using ART (annual trailing-ish) values:
      - epsusd (earnings per share) for PE
      - bvps (book value per share) for PB
      - dps (dividends per share) for dividend yield
      - debtnc (non-current debt; proxy for long-term debt)
      - equity (shareholders' equity)
    """
    df = fetch_datatable_all_rows(
        "SHARADAR/SF1",
        params={
            "dimension": "ART",
            "qopts.columns": "ticker,calendardate,lastupdated,epsusd,bvps,dps,debtnc,equity",
            "qopts.per_page": 10000,
        },
        max_pages=500,
        timeout=180,
    )

    needed = {"ticker", "calendardate", "lastupdated", "epsusd", "bvps", "dps", "debtnc", "equity"}
    missing = needed - set(df.columns)
    if missing:
        raise SystemExit(f"SF1 missing required columns: {sorted(missing)}")

    df["calendardate"] = pd.to_datetime(df["calendardate"], errors="coerce")
    df["lastupdated"] = pd.to_datetime(df["lastupdated"], errors="coerce")

    # Pick most recent row per ticker
    df = df.sort_values(["ticker", "calendardate", "lastupdated"])
    df = df.groupby("ticker", as_index=False).tail(1)

    for c in ["epsusd", "bvps", "dps", "debtnc", "equity"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    return df


def main() -> None:
    # 1) "Current" prices (latest date in your available SEP slice)
    latest_price_date = get_latest_sep_date_no_sort()
    closes = load_sep_closes(latest_price_date)

    # 2) Latest fundamentals per ticker
    sf1 = load_sf1_latest_art()

    # 3) Merge universe
    df = closes.merge(sf1, on="ticker", how="inner")

    # 4) Compute CURRENT ratios from current price
    df["pe"] = df["close"] / df["epsusd"]
    df["pb"] = df["close"] / df["bvps"]
    df["div_yield"] = df["dps"] / df["close"]
    df["lt_debt_equity"] = df["debtnc"] / df["equity"]

    for c in ["pe", "pb", "div_yield", "lt_debt_equity"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # 5) Wilson's Algorithm rules
    df["rule_div"] = df["div_yield"].notna() & (df["div_yield"] >= TREASURY_10Y)
    df["rule_pe"] = df["pe"].notna() & (df["pe"] <= 13)
    df["rule_pb"] = df["pb"].notna() & (df["pb"] <= 1)
    df["rule_ltde"] = df["lt_debt_equity"].notna() & (df["lt_debt_equity"] <= 1)
    df["passes"] = df["rule_div"] & df["rule_pe"] & df["rule_pb"] & df["rule_ltde"]

    winners = df[df["passes"]].copy().sort_values("div_yield", ascending=False)

    pass_list = []
    for _, r in winners.iterrows():
        pass_list.append({
            "ticker": r["ticker"],
            "price": round(float(r["close"]), 2),
            "pe": None if pd.isna(r["pe"]) else round(float(r["pe"]), 4),
            "pb": None if pd.isna(r["pb"]) else round(float(r["pb"]), 4),
            "divYield": None if pd.isna(r["div_yield"]) else round(float(r["div_yield"]), 6),
            "ltDebtEquity": None if pd.isna(r["lt_debt_equity"]) else round(float(r["lt_debt_equity"]), 4),
            "priceDate": str(latest_price_date),
        })

    stats = {
        "asOfPriceDate": str(latest_price_date),
        "treasuryYield10y": TREASURY_10Y,
        "sepRows": int(len(closes)),
        "sf1Rows": int(len(sf1)),
        "mergedRows": int(len(df)),
        "div_rule_pass": int(df["rule_div"].sum()),
        "pe_rule_pass": int(df["rule_pe"].sum()),
        "pb_rule_pass": int(df["rule_pb"].sum()),
        "ltde_rule_pass": int(df["rule_ltde"].sum()),
        "all_rules_pass": int(df["passes"].sum()),
    }

    # Include near-misses so you can see the closest stocks even if pass=0
    df["rules_passed"] = (
        df["rule_div"].astype(int) +
        df["rule_pe"].astype(int) +
        df["rule_pb"].astype(int) +
        df["rule_ltde"].astype(int)
    )
    top = df.sort_values(["rules_passed", "div_yield"], ascending=[False, False]).head(50)

    top_candidates = []
    for _, r in top.iterrows():
        top_candidates.append({
            "ticker": r["ticker"],
            "rulesPassed": int(r["rules_passed"]),
            "price": None if pd.isna(r["close"]) else round(float(r["close"]), 2),
            "pe": None if pd.isna(r["pe"]) else round(float(r["pe"]), 4),
            "pb": None if pd.isna(r["pb"]) else round(float(r["pb"]), 4),
            "divYield": None if pd.isna(r["div_yield"]) else round(float(r["div_yield"]), 6),
            "ltDebtEquity": None if pd.isna(r["lt_debt_equity"]) else round(float(r["lt_debt_equity"]), 4),
            "priceDate": str(latest_price_date),
        })

    out = {
        "runDate": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "stats": stats,
        "pass": pass_list,
        "topCandidates": top_candidates
    }

    with open("passlist.json", "w") as f:
        json.dump(out, f, indent=2)

    print("Stats:", stats)
    print(f"Wrote passlist.json with {len(pass_list)} PASS tickers")


if __name__ == "__main__":
    main()
