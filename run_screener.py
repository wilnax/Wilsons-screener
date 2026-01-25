import os
import json
import datetime as dt
import time
import requests
import pandas as pd

FMP_API_KEY = os.environ.get("FMP_API_KEY")
if not FMP_API_KEY:
    raise SystemExit("Missing FMP_API_KEY (set it as a GitHub Secret).")

# Example: 0.0423 for 4.23%
TREASURY_10Y = float(os.environ.get("TREASURY_10Y", "0.0423"))

# FMP Stock Screener endpoint (stable)
SCREENER_URL = "https://financialmodelingprep.com/stable/company-screener"  # docs  [oai_citation:1‡Financial Modeling Prep](https://site.financialmodelingprep.com/developer/docs/stable/search-company-screener)

# We only want US-listed common stocks (NYSE/NASDAQ/AMEX).
# Note: FMP naming can vary; we will filter again client-side for safety.
ALLOWED_EXCHANGES = {"NYSE", "NASDAQ", "AMEX"}


def _get_json(url: str, params: dict, timeout: int = 180) -> dict | list:
    r = requests.get(url, params=params, timeout=timeout)
    if not r.ok:
        snippet = (r.text or "")[:2000]
        print(f"HTTP {r.status_code} error for {url}\nResponse snippet:\n{snippet}\n")
        r.raise_for_status()
    return r.json()


def _to_float(x):
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _normalize_div_yield(dy):
    """
    FMP sometimes returns dividendYield as a fraction (0.034) or percent (3.4).
    Normalize to fraction.
    """
    if dy is None:
        return None
    dy = _to_float(dy)
    if dy is None:
        return None
    if dy > 1.0:
        return dy / 100.0
    return dy


def fetch_candidates_from_fmp() -> pd.DataFrame:
    """
    Use the server-side screener to reduce the universe in ONE call.
    We still re-check rules locally to avoid any parameter-name quirks.
    """
    params = {
        "apikey": FMP_API_KEY,

        # US companies only
        "country": "US",

        # Keep it big enough to capture all matches
        "limit": 10000,

        # Try to pre-filter with common fields.
        # If FMP ignores some of these, our local re-check still enforces Wilson's rules.
        "isEtf": "false",
        "isFund": "false",

        # Common screener filter names used by many FMP examples/articles
        "peLowerThan": 13,
        "priceToBookRatioLowerThan": 1,
        "debtToEquityLowerThan": 1,
        "dividendYieldMoreThan": TREASURY_10Y,
    }

    data = _get_json(SCREENER_URL, params, timeout=240)

    if not isinstance(data, list):
        raise SystemExit(f"Unexpected screener response type: {type(data)}")

    if len(data) == 0:
        # Not necessarily wrong; could be strict thresholds.
        # But we’ll output stats and a topCandidates list from near-misses (later).
        return pd.DataFrame()

    df = pd.DataFrame(data)
    return df


def main() -> None:
    run_date = dt.datetime.now(dt.timezone.utc).isoformat()

    raw = fetch_candidates_from_fmp()

    if raw.empty:
        out = {
            "runDate": run_date,
            "stats": {
                "country": "US",
                "treasuryYield10y": TREASURY_10Y,
                "rowsFromScreener": 0,
                "afterLocalFilter": 0,
                "all_rules_pass": 0,
                "notes": "Screener returned 0 rows. Either no matches, or the endpoint fields/filters differ. Upgrade/plan is OK if no 402."
            },
            "pass": [],
            "topCandidates": []
        }
        with open("passlist.json", "w") as f:
            json.dump(out, f, indent=2)
        pd.DataFrame([]).to_csv("passlist.csv", index=False)
        print("Wrote passlist.json (0 rows)")
        return

    # Normalize column names we care about (FMP field names can vary)
    # We'll try several possibilities and then compute/rename to standard names.
    cols = set(raw.columns)

    def pick(*candidates):
        for c in candidates:
            if c in cols:
                return c
        return None

    sym_col = pick("symbol", "ticker")
    price_col = pick("price", "stockPrice")
    exch_col = pick("exchange", "exchangeShortName", "exchangeShort")
    pe_col = pick("pe", "peRatio", "priceEarningsRatio", "priceToEarningsRatio")
    pb_col = pick("pb", "pbRatio", "priceToBookRatio", "priceToBookRatioTTM")
    dy_col = pick("dividendYield", "dividendYieldTTM", "divYield")
    de_col = pick("debtToEquity", "debtToEquityRatio", "debtEquity", "debtEquityRatio")

    # If we can't find the basics, print columns for debugging
    missing = [name for name, col in [
        ("symbol", sym_col),
        ("price", price_col),
        ("exchange", exch_col),
        ("pe", pe_col),
        ("pb", pb_col),
        ("dividendYield", dy_col),
        ("debtToEquity", de_col),
    ] if col is None]

    if missing:
        print("Columns returned by FMP screener:")
        print(sorted(list(cols)))
        raise SystemExit(f"Missing required columns: {missing}")

    df = raw.rename(columns={
        sym_col: "ticker",
        price_col: "price",
        exch_col: "exchange",
        pe_col: "pe",
        pb_col: "pb",
        dy_col: "divYield",
        de_col: "debtEquity",
    }).copy()

    # Clean and normalize
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df["exchange"] = df["exchange"].astype(str).str.upper().str.strip()

    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["pe"] = pd.to_numeric(df["pe"], errors="coerce")
    df["pb"] = pd.to_numeric(df["pb"], errors="coerce")
    df["debtEquity"] = pd.to_numeric(df["debtEquity"], errors="coerce")
    df["divYield"] = df["divYield"].apply(_normalize_div_yield)

    # Filter to US exchanges we care about (some endpoints include OTC, etc.)
    df = df[df["exchange"].isin(ALLOWED_EXCHANGES)].copy()

    # Local enforcement of Wilson's Algorithm
    df["rule_div"] = df["divYield"].notna() & (df["divYield"] >= TREASURY_10Y)
    df["rule_pe"] = df["pe"].notna() & (df["pe"] <= 13)
    df["rule_pb"] = df["pb"].notna() & (df["pb"] <= 1)
    df["rule_de"] = df["debtEquity"].notna() & (df["debtEquity"] <= 1)

    df["passes"] = df["rule_div"] & df["rule_pe"] & df["rule_pb"] & df["rule_de"]
    df["rulesPassed"] = (
        df["rule_div"].astype(int) +
        df["rule_pe"].astype(int) +
        df["rule_pb"].astype(int) +
        df["rule_de"].astype(int)
    )

    winners = df[df["passes"]].copy().sort_values(
        ["divYield", "pe", "pb"],
        ascending=[False, True, True]
    )

    pass_list = []
    for _, r in winners.iterrows():
        pass_list.append({
            "ticker": r["ticker"],
            "exchange": r["exchange"],
            "price": None if pd.isna(r["price"]) else round(float(r["price"]), 2),
            "divYield": None if r["divYield"] is None or pd.isna(r["divYield"]) else round(float(r["divYield"]), 6),
            "pe": None if pd.isna(r["pe"]) else round(float(r["pe"]), 4),
            "pb": None if pd.isna(r["pb"]) else round(float(r["pb"]), 4),
            "debtEquity": None if pd.isna(r["debtEquity"]) else round(float(r["debtEquity"]), 4),
        })

    # Near-misses: best 60 by rulesPassed then dividend yield
    top = df.sort_values(["rulesPassed", "divYield"], ascending=[False, False]).head(60)
    top_candidates = []
    for _, r in top.iterrows():
        top_candidates.append({
            "ticker": r["ticker"],
            "exchange": r["exchange"],
            "rulesPassed": int(r["rulesPassed"]),
            "price": None if pd.isna(r["price"]) else round(float(r["price"]), 2),
            "divYield": None if r["divYield"] is None or pd.isna(r["divYield"]) else round(float(r["divYield"]), 6),
            "pe": None if pd.isna(r["pe"]) else round(float(r["pe"]), 4),
            "pb": None if pd.isna(r["pb"]) else round(float(r["pb"]), 4),
            "debtEquity": None if pd.isna(r["debtEquity"]) else round(float(r["debtEquity"]), 4),
        })

    stats = {
        "country": "US",
        "treasuryYield10y": TREASURY_10Y,
        "rowsFromScreener": int(len(raw)),
        "afterExchangeFilter": int(len(df)),
        "afterLocalFilter": int(len(df)),
        "div_rule_pass": int(df["rule_div"].sum()),
        "pe_rule_pass": int(df["rule_pe"].sum()),
        "pb_rule_pass": int(df["rule_pb"].sum()),
        "de_rule_pass": int(df["rule_de"].sum()),
        "all_rules_pass": int(df["passes"].sum()),
        "exchangesIncluded": sorted(list(ALLOWED_EXCHANGES)),
    }

    out = {
        "runDate": run_date,
        "stats": stats,
        "pass": pass_list,
        "topCandidates": top_candidates
    }

    with open("passlist.json", "w") as f:
        json.dump(out, f, indent=2)

    # CSV for easy viewing
    pd.DataFrame(pass_list).to_csv("passlist.csv", index=False)

    print("Stats:", stats)
    print(f"Wrote passlist.json and passlist.csv with {len(pass_list)} PASS tickers")


if __name__ == "__main__":
    main()
