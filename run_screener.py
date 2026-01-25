import os
import json
import datetime as dt
import time
import requests
import pandas as pd

FMP_API_KEY = os.environ.get("FMP_API_KEY")
if not FMP_API_KEY:
    raise SystemExit("Missing FMP_API_KEY (GitHub Secret).")

TREASURY_10Y = float(os.environ.get("TREASURY_10Y", "0.0423"))

COMPANY_SCREENER_URL = "https://financialmodelingprep.com/stable/company-screener"
RATIOS_TTM_URL = "https://financialmodelingprep.com/stable/ratios-ttm"
KEY_METRICS_TTM_URL = "https://financialmodelingprep.com/stable/key-metrics-ttm"

ALLOWED_EXCHANGES = {"NYSE", "NASDAQ", "AMEX"}
SLEEP_BETWEEN_CALLS_SEC = 0.12
MAX_TICKERS_TO_EVALUATE = 2500


def _get_json(url: str, params: dict, timeout: int = 180):
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
    dy = _to_float(dy)
    if dy is None:
        return None
    if dy > 1.0:
        return dy / 100.0
    return dy


def _pick(d: dict, keys):
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return None


def fetch_us_universe() -> pd.DataFrame:
    params = {
        "apikey": FMP_API_KEY,
        "country": "US",
        "isEtf": "false",
        "isFund": "false",
        "isActivelyTrading": "true",
        "limit": 10000,
    }
    data = _get_json(COMPANY_SCREENER_URL, params, timeout=240)
    if not isinstance(data, list) or len(data) == 0:
        raise SystemExit("company-screener returned empty.")
    return pd.DataFrame(data)


def main() -> None:
    run_date = dt.datetime.now(dt.timezone.utc).isoformat()

    raw = fetch_us_universe()

    if "symbol" not in raw.columns or "price" not in raw.columns:
        raise SystemExit(f"Missing symbol/price. Columns: {list(raw.columns)}")

    # Pick ONE exchange column safely
    exch_candidates = [c for c in ["exchangeShortName", "exchange"] if c in raw.columns]
    if not exch_candidates:
        raise SystemExit(f"No exchange column found. Columns: {list(raw.columns)}")
    exch_col = exch_candidates[0]

    # Pick dividend column
    if "lastAnnualDividend" not in raw.columns:
        raise SystemExit("Missing lastAnnualDividend in company-screener.")
    div_col = "lastAnnualDividend"

    df = raw[["symbol", "price", exch_col, div_col]].copy()
    df = df.rename(columns={exch_col: "exchange", div_col: "lastAnnualDividend"})

    df["ticker"] = df["symbol"].astype(str).str.upper().str.strip()
    df["exchange"] = df["exchange"].astype(str).str.upper().str.strip()
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["lastAnnualDividend"] = pd.to_numeric(df["lastAnnualDividend"], errors="coerce")

    df = df[df["exchange"].isin(ALLOWED_EXCHANGES)]
    df = df.dropna(subset=["ticker", "price"])
    df = df[df["price"] > 0]

    df["divYield_est"] = (df["lastAnnualDividend"] / df["price"]).apply(_normalize_div_yield)

    stage1 = df[df["divYield_est"].notna() & (df["divYield_est"] >= TREASURY_10Y)]
    stage1 = stage1.sort_values("divYield_est", ascending=False).head(MAX_TICKERS_TO_EVALUATE)

    results = []
    errors = 0

    for _, r in stage1.iterrows():
        sym = r["ticker"]
        px = float(r["price"])
        exch = r["exchange"]

        ratios = _get_json(RATIOS_TTM_URL, {"apikey": FMP_API_KEY, "symbol": sym})
        time.sleep(SLEEP_BETWEEN_CALLS_SEC)
        metrics = _get_json(KEY_METRICS_TTM_URL, {"apikey": FMP_API_KEY, "symbol": sym})
        time.sleep(SLEEP_BETWEEN_CALLS_SEC)

        pe = _to_float(_pick(ratios[0], ["peRatioTTM", "peRatio"])) if ratios else None
        pb = _to_float(_pick(ratios[0], ["priceToBookRatioTTM", "priceToBookRatio"])) if ratios else None
        de = _to_float(_pick(ratios[0], ["debtEquityRatioTTM", "debtEquityRatio"])) if ratios else None
        dy = _normalize_div_yield(_pick(ratios[0], ["dividendYieldTTM", "dividendYield"])) if ratios else None

        if metrics:
            m = metrics[0]
            if pe is None: pe = _to_float(_pick(m, ["peRatioTTM", "peRatio"]))
            if pb is None: pb = _to_float(_pick(m, ["pbRatioTTM", "pbRatio"]))
            if de is None: de = _to_float(_pick(m, ["debtEquityRatioTTM", "debtEquityRatio"]))
            if dy is None: dy = _normalize_div_yield(_pick(m, ["dividendYieldTTM", "dividendYield"]))

        if dy is None:
            dy = r["divYield_est"]

        if pe is None or pb is None or de is None or dy is None:
            errors += 1
            continue

        rule_div = dy >= TREASURY_10Y
        rule_pe = pe <= 13
        rule_pb = pb <= 1
        rule_de = de <= 1

        results.append({
            "ticker": sym,
            "exchange": exch,
            "price": round(px, 2),
            "divYield": round(float(dy), 6),
            "pe": round(float(pe), 4),
            "pb": round(float(pb), 4),
            "debtEquity": round(float(de), 4),
            "rule_div": rule_div,
            "rule_pe": rule_pe,
            "rule_pb": rule_pb,
            "rule_de": rule_de,
            "passes": rule_div and rule_pe and rule_pb and rule_de,
        })

    out = pd.DataFrame(results)
    pass_df = out[out["passes"]] if not out.empty else out
    top_df = out.copy()
    if not out.empty:
        out["rulesPassed"] = (
            out["rule_div"].astype(int) +
            out["rule_pe"].astype(int) +
            out["rule_pb"].astype(int) +
            out["rule_de"].astype(int)
        )
        top_df = out.sort_values(["rulesPassed", "divYield"], ascending=[False, False]).head(60)

    stats = {
        "treasuryYield10y": TREASURY_10Y,
        "universe": int(len(df)),
        "stage1_prefilter": int(len(stage1)),
        "evaluated": int(len(out)),
        "skipped_missing": int(errors),
        "all_rules_pass": int(len(pass_df)),
    }

    payload = {
        "runDate": run_date,
        "stats": stats,
        "pass": pass_df.drop(columns=["passes"]).to_dict(orient="records") if not pass_df.empty else [],
        "topCandidates": top_df.drop(columns=["passes"], errors="ignore").to_dict(orient="records") if not top_df.empty else [],
    }

    with open("passlist.json", "w") as f:
        json.dump(payload, f, indent=2)

    pass_df.to_csv("passlist.csv", index=False)

    print("Stats:", stats)
    print(f"Wrote passlist.json with {len(pass_df)} PASS tickers")


if __name__ == "__main__":
    main()
