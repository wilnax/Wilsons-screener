import os, json, time, requests, pandas as pd
from datetime import datetime, timezone

FMP_API_KEY = os.environ["FMP_API_KEY"]
TREASURY_10Y = float(os.environ.get("TREASURY_10Y", "0.0423"))

COMPANY_SCREENER = "https://financialmodelingprep.com/stable/company-screener"
RATIOS_TTM = "https://financialmodelingprep.com/stable/ratios-ttm"

ALLOWED_EXCHANGES = {"NYSE", "NASDAQ", "AMEX"}
SLEEP = 0.12
MAX = 1200

def get(url, p):
    r = requests.get(url, params=p, timeout=180)
    if not r.ok:
        print(r.text[:500])
        r.raise_for_status()
    return r.json()

def to_float(x):
    try:
        return float(x)
    except:
        return None

def norm_pct_or_frac(x):
    """Normalize yields that might come back as % (e.g., 4.2) or fraction (0.042)."""
    x = to_float(x)
    if x is None:
        return None
    if x > 1:
        return x / 100.0
    return x

def main():
    run_date = datetime.now(timezone.utc).isoformat()

    universe = get(COMPANY_SCREENER, {
        "apikey": FMP_API_KEY,
        "country": "US",
        "isEtf": "false",
        "isFund": "false",
        "isActivelyTrading": "true",
        "limit": 10000
    })
    df = pd.DataFrame(universe)

    exch = "exchangeShortName" if "exchangeShortName" in df.columns else "exchange"
    df = df[["symbol", "price", exch, "lastAnnualDividend"]].rename(columns={exch: "exchange"})
    df["ticker"] = df["symbol"].astype(str).str.upper().str.strip()
    df["exchange"] = df["exchange"].astype(str).str.upper().str.strip()
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["lastAnnualDividend"] = pd.to_numeric(df["lastAnnualDividend"], errors="coerce")

    df = df[df["exchange"].isin(ALLOWED_EXCHANGES)]
    df = df.dropna(subset=["ticker", "price"])
    df = df[df["price"] > 0]

    # Estimate yield to prefilter
    df["divYield_est"] = (df["lastAnnualDividend"] / df["price"]).apply(norm_pct_or_frac)
    stage1 = df[df["divYield_est"].notna() & (df["divYield_est"] >= TREASURY_10Y)] \
                .sort_values("divYield_est", ascending=False) \
                .head(MAX)

    results, skipped = [], 0

    for _, r in stage1.iterrows():
        sym = r["ticker"]
        px = float(r["price"])

        data = get(RATIOS_TTM, {"apikey": FMP_API_KEY, "symbol": sym})
        time.sleep(SLEEP)

        if not data or not isinstance(data, list) or not isinstance(data[0], dict):
            skipped += 1
            continue

        d = data[0]

        # Pull metrics (these are the exact keys you showed)
        pe = to_float(d.get("priceToEarningsRatioTTM"))
        pb = to_float(d.get("priceToBookRatioTTM"))
        de = to_float(d.get("debtToEquityRatioTTM"))
        dy = norm_pct_or_frac(d.get("dividendYieldTTM"))

        # Yield fallback if API doesn't provide it
        if dy is None:
            dy = norm_pct_or_frac(r["divYield_est"])

        # NEW: reject negative or zero PE / PB
        # (Also reject missing)
        if pe is None or pb is None or de is None or dy is None:
            skipped += 1
            continue
        if pe <= 0 or pb <= 0:
            skipped += 1
            continue

        rule_div = dy >= TREASURY_10Y
        rule_pe = pe <= 13
        rule_pb = pb <= 1
        rule_de = de <= 1

        results.append({
            "ticker": sym,
            "price": round(px, 2),
            "divYield": round(dy, 6),
            "pe": round(pe, 4),
            "pb": round(pb, 4),
            "debtEquity": round(de, 4),
            "rule_div": rule_div,
            "rule_pe": rule_pe,
            "rule_pb": rule_pb,
            "rule_de": rule_de,
            "passes": rule_div and rule_pe and rule_pb and rule_de
        })

    out = pd.DataFrame(results)
    passed = out[out["passes"]].sort_values(["divYield", "pe"], ascending=[False, True]) if not out.empty else out

    stats = {
        "treasuryYield10y": TREASURY_10Y,
        "universe": int(len(df)),
        "stage1_prefilter": int(len(stage1)),
        "evaluated": int(len(out)),
        "skipped": int(skipped),
        "all_rules_pass": int(len(passed))
    }

    payload = {
        "runDate": run_date,
        "stats": stats,
        "pass": passed.drop(columns=["passes"], errors="ignore").to_dict("records"),
        "topCandidates": out.sort_values("divYield", ascending=False).head(50).to_dict("records") if not out.empty else []
    }

    with open("passlist.json", "w") as f:
        json.dump(payload, f, indent=2)
    passed.to_csv("passlist.csv", index=False)

    print(stats)

if __name__ == "__main__":
    main()
