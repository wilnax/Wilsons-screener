import os, json, time, requests, pandas as pd
from datetime import datetime, timezone

FMP_API_KEY = os.environ["FMP_API_KEY"]
TREASURY_10Y = float(os.environ.get("TREASURY_10Y", "0.0423"))

COMPANY_SCREENER = "https://financialmodelingprep.com/stable/company-screener"
RATIOS_TTM = "https://financialmodelingprep.com/stable/ratios-ttm"

ALLOWED_EXCHANGES = {"NYSE","NASDAQ","AMEX"}
SLEEP = 0.12
MAX = 1200

def get(url, p):
    r = requests.get(url, params=p, timeout=180)
    if not r.ok:
        print(r.text[:500])
        r.raise_for_status()
    return r.json()

def norm(x):
    try:
        x = float(x)
        if x > 1: x /= 100
        return x
    except:
        return None

def main():
    run_date = datetime.now(timezone.utc).isoformat()

    universe = get(COMPANY_SCREENER, {
        "apikey": FMP_API_KEY,
        "country":"US","isEtf":"false","isFund":"false","isActivelyTrading":"true","limit":10000
    })
    df = pd.DataFrame(universe)

    exch = "exchangeShortName" if "exchangeShortName" in df.columns else "exchange"
    df = df[["symbol","price",exch,"lastAnnualDividend"]].rename(columns={exch:"exchange"})
    df["ticker"] = df["symbol"].str.upper().str.strip()
    df["exchange"] = df["exchange"].str.upper().str.strip()
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["lastAnnualDividend"] = pd.to_numeric(df["lastAnnualDividend"], errors="coerce")

    df = df[df["exchange"].isin(ALLOWED_EXCHANGES)]
    df = df.dropna(subset=["ticker","price"])
    df = df[df["price"]>0]

    df["divYield_est"] = (df["lastAnnualDividend"]/df["price"]).apply(norm)
    stage1 = df[df["divYield_est"] >= TREASURY_10Y].sort_values("divYield_est", ascending=False).head(MAX)

    results, skipped = [], 0

    for _, r in stage1.iterrows():
        sym = r["ticker"]
        px = float(r["price"])

        data = get(RATIOS_TTM, {"apikey":FMP_API_KEY,"symbol":sym})
        time.sleep(SLEEP)

        if not data or not isinstance(data,list):
            skipped += 1
            continue

        d = data[0]

        pe = norm(d.get("priceToEarningsRatioTTM"))
        pb = norm(d.get("priceToBookRatioTTM"))
        de = norm(d.get("debtToEquityRatioTTM"))
        dy = norm(d.get("dividendYieldTTM")) or norm(r["divYield_est"])

        if None in (pe,pb,de,dy):
            skipped += 1
            continue

        rule_div = dy >= TREASURY_10Y
        rule_pe = pe <= 13
        rule_pb = pb <= 1
        rule_de = de <= 1

        results.append({
            "ticker":sym,
            "price":round(px,2),
            "divYield":round(dy,4),
            "pe":round(pe,4),
            "pb":round(pb,4),
            "debtEquity":round(de,4),
            "rule_div":rule_div,
            "rule_pe":rule_pe,
            "rule_pb":rule_pb,
            "rule_de":rule_de,
            "passes":rule_div and rule_pe and rule_pb and rule_de
        })

    out = pd.DataFrame(results)
    passed = out[out["passes"]]

    stats = {
        "treasuryYield10y":TREASURY_10Y,
        "universe":len(df),
        "stage1_prefilter":len(stage1),
        "evaluated":len(out),
        "skipped":skipped,
        "all_rules_pass":len(passed)
    }

    payload = {
        "runDate":run_date,
        "stats":stats,
        "pass":passed.drop(columns=["passes"]).to_dict("records"),
        "topCandidates":out.sort_values("divYield",ascending=False).head(50).to_dict("records")
    }

    with open("passlist.json","w") as f:
        json.dump(payload,f,indent=2)
    passed.to_csv("passlist.csv",index=False)

    print(stats)

if __name__ == "__main__":
    main()
