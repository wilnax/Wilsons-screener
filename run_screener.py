import os
import json
import datetime as dt
import requests
import pandas as pd

FMP_API_KEY = os.environ.get("FMP_API_KEY")
if not FMP_API_KEY:
    raise SystemExit("Missing FMP_API_KEY (GitHub Secret).")

# 4.23% => 0.0423
TREASURY_10Y = float(os.environ.get("TREASURY_10Y", "0.0423"))

# Legacy API v3 stock screener (returns valuation/leverage/yield fields in the payload)
SCREENER_URL = "https://financialmodelingprep.com/api/v3/stock-screener"

ALLOWED_EXCHANGES = {"NYSE", "NASDAQ", "AMEX"}


def _get_json(url: str, params: dict, timeout: int = 240):
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
    FMP sometimes returns dividendYield as 0.034 (fraction) or 3.4 (percent).
    Normalize to fraction.
    """
    dy = _to_float(dy)
    if dy is None:
        return None
    if dy > 1.0:
        return dy / 100.0
    return dy


def main() -> None:
    run_date = dt.datetime.now(dt.timezone.utc).isoformat()

    # Ask the server to do the heavy filtering
    # Common param names for this endpoint:
    #   - country=US
    #   - exchange=NASDAQ,NYSE,AMEX   (sometimes "exchange" is required, sometimes ignored)
    #   - dividendMoreThan=...
    #   - peLowerThan=...
    #   - priceToBookRatioLowerThan=...
    #   - debtToEquityLowerThan=...
    params = {
        "apikey": FMP_API_KEY,
        "country": "US",
        "exchange": "NASDAQ,NYSE,AMEX",
        "limit": 10000,

        "dividendMoreThan": TREASURY_10Y,
        "peLowerThan": 13,
        "priceToBookRatioLowerThan": 1,
        "debtToEquityLowerThan": 1,
    }

    data = _get_json(SCREENER_URL, params, timeout=240)

    if not isinstance(data, list):
        raise SystemExit(f"Unexpected response type from stock-screener: {type(data)}")

    if len(data) == 0:
        out = {
            "runDate": run_date,
            "stats": {
                "treasuryYield10y": TREASURY_10Y,
                "rowsFromApi": 0,
                "afterLocalFilter": 0,
                "all_rules_pass": 0,
                "notes": "API returned 0 rows after server-side filters. Could be strict thresholds or different field availability."
            },
            "pass": [],
            "topCandidates": []
        }
        with open("passlist.json", "w") as f:
            json.dump(out, f, indent=2)
        pd.DataFrame([]).to_csv("passlist.csv", index=False)
        print("Wrote passlist.json (0 rows)")
        return

    df = pd.DataFrame(data)

    # Print columns once in Actions logs if something goes sideways
    cols = set(df.columns)

    def need(colname: str):
        if colname not in cols:
            print("Columns returned:", sorted(list(cols)))
            raise SystemExit(f"Missing required column '{colname}' in stock-screener response.")

    # Required fields we expect from stock-screener
    need("symbol")
    need("price")

    # These are sometimes named slightly differently; try a few options
    pe_col = "pe" if "pe" in cols else ("peRatio" if "peRatio" in cols else None)
    pb_col = "priceToBookRatio" if "priceToBookRatio" in cols else ("pb" if "pb" in cols else None)
    dy_col = "dividendYield" if "dividendYield" in cols else ("divYield" if "divYield" in cols else None)
    de_col = "debtToEquity" if "debtToEquity" in cols else ("debtEquityRatio" if "debtEquityRatio" in cols else None)

    missing = [name for name, col in [
        ("pe", pe_col),
        ("pb", pb_col),
        ("dividendYield", dy_col),
        ("debtToEquity", de_col),
    ] if col is None]

    if missing:
        print("Columns returned:", sorted(list(cols)))
        raise SystemExit(
            "The stock-screener endpoint did not return the ratio fields we need: "
            f"{missing}. If your plan excludes these, we can compute them using other endpoints."
        )

    # Normalize
    df = df.rename(columns={
        "symbol": "ticker",
        pe_col: "pe",
        pb_col: "pb",
        dy_col: "divYield",
        de_col: "debtEquity",
    }).copy()

    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    if "exchangeShortName" in cols:
        df["exchange"] = df["exchangeShortName"].astype(str).str.upper().str.strip()
    elif "exchange" in cols:
        df["exchange"] = df["exchange"].astype(str).str.upper().str.strip()
    else:
        df["exchange"] = ""

    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["pe"] = pd.to_numeric(df["pe"], errors="coerce")
    df["pb"] = pd.to_numeric(df["pb"], errors="coerce")
    df["debtEquity"] = pd.to_numeric(df["debtEquity"], errors="coerce")
    df["divYield"] = df["divYield"].apply(_normalize_div_yield)

    # Keep only main US exchanges if exchange info exists
    if df["exchange"].str.len().gt(0).any():
        df = df[df["exchange"].isin(ALLOWED_EXCHANGES)].copy()

    # Local enforcement (so we control pass/fail)
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

    winners = df[df["passes"]].copy().sort_values(["divYield", "pe"], ascending=[False, True])

    pass_list = []
    for _, r in winners.iterrows():
        pass_list.append({
            "ticker": r["ticker"],
            "exchange": r["exchange"] if isinstance(r["exchange"], str) else "",
            "price": None if pd.isna(r["price"]) else round(float(r["price"]), 2),
            "divYield": None if pd.isna(r["divYield"]) else round(float(r["divYield"]), 6),
            "pe": None if pd.isna(r["pe"]) else round(float(r["pe"]), 4),
            "pb": None if pd.isna(r["pb"]) else round(float(r["pb"]), 4),
            "debtEquity": None if pd.isna(r["debtEquity"]) else round(float(r["debtEquity"]), 4),
        })

    top = df.sort_values(["rulesPassed", "divYield"], ascending=[False, False]).head(60)
    top_candidates = []
    for _, r in top.iterrows():
        top_candidates.append({
            "ticker": r["ticker"],
            "exchange": r["exchange"] if isinstance(r["exchange"], str) else "",
            "rulesPassed": int(r["rulesPassed"]),
            "price": None if pd.isna(r["price"]) else round(float(r["price"]), 2),
            "divYield": None if pd.isna(r["divYield"]) else round(float(r["divYield"]), 6),
            "pe": None if pd.isna(r["pe"]) else round(float(r["pe"]), 4),
            "pb": None if pd.isna(r["pb"]) else round(float(r["pb"]), 4),
            "debtEquity": None if pd.isna(r["debtEquity"]) else round(float(r["debtEquity"]), 4),
        })

    stats = {
        "treasuryYield10y": TREASURY_10Y,
        "rowsFromApi": int(len(df)),
        "div_rule_pass": int(df["rule_div"].sum()),
        "pe_rule_pass": int(df["rule_pe"].sum()),
        "pb_rule_pass": int(df["rule_pb"].sum()),
        "de_rule_pass": int(df["rule_de"].sum()),
        "all_rules_pass": int(df["passes"].sum()),
    }

    out = {
        "runDate": run_date,
        "stats": stats,
        "pass": pass_list,
        "topCandidates": top_candidates
    }

    with open("passlist.json", "w") as f:
        json.dump(out, f, indent=2)

    pd.DataFrame(pass_list).to_csv("passlist.csv", index=False)

    print("Stats:", stats)
    print(f"Wrote passlist.json and passlist.csv with {len(pass_list)} PASS tickers")


if __name__ == "__main__":
    main()
