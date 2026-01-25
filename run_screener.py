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
MAX_TICKERS_TO_EVALUATE = 1200  # raise later after this works
DEBUG_SAVE_FIRST_N = 3          # saves first 3 raw payloads to json files


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
    # normalize percent -> fraction
    if dy > 1.0:
        return dy / 100.0
    return dy


def _pick(d: dict, keys):
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return None


def _extract_first_record(payload):
    """
    Stable endpoints may return:
      - dict of fields (record)
      - list of dicts (records)
      - dict wrapper that contains list under keys like data/result/metrics/ratios/items
    We normalize to a single dict record or None.
    """
    if payload is None:
        return None

    # Case 1: list of dicts
    if isinstance(payload, list):
        if len(payload) > 0 and isinstance(payload[0], dict):
            return payload[0]
        return None

    # Case 2: dict record or dict wrapper
    if isinstance(payload, dict):
        # If it's already a record (has typical fields), return it as-is
        # Otherwise, try common wrapper keys that hold a list of dicts
        wrapper_keys = ["data", "result", "results", "items", "metrics", "ratios"]
        for k in wrapper_keys:
            if k in payload and isinstance(payload[k], list) and len(payload[k]) > 0 and isinstance(payload[k][0], dict):
                return payload[k][0]

        # Sometimes wrapper contains nested dict with list inside
        for k in wrapper_keys:
            if k in payload and isinstance(payload[k], dict):
                inner = payload[k]
                for kk in wrapper_keys:
                    if kk in inner and isinstance(inner[kk], list) and len(inner[kk]) > 0 and isinstance(inner[kk][0], dict):
                        return inner[kk][0]

        # If it’s a dict but not a wrapper we recognize, treat it as record
        return payload

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


def fetch_ratios_ttm(symbol: str):
    payload = _get_json(RATIOS_TTM_URL, {"apikey": FMP_API_KEY, "symbol": symbol}, timeout=180)
    time.sleep(SLEEP_BETWEEN_CALLS_SEC)
    return payload


def fetch_key_metrics_ttm(symbol: str):
    payload = _get_json(KEY_METRICS_TTM_URL, {"apikey": FMP_API_KEY, "symbol": symbol}, timeout=180)
    time.sleep(SLEEP_BETWEEN_CALLS_SEC)
    return payload


def main() -> None:
    run_date = dt.datetime.now(dt.timezone.utc).isoformat()

    raw = fetch_us_universe()

    if "symbol" not in raw.columns or "price" not in raw.columns:
        raise SystemExit(f"Missing symbol/price. Columns: {list(raw.columns)}")

    exch_candidates = [c for c in ["exchangeShortName", "exchange"] if c in raw.columns]
    if not exch_candidates:
        raise SystemExit(f"No exchange column found. Columns: {list(raw.columns)}")
    exch_col = exch_candidates[0]

    if "lastAnnualDividend" not in raw.columns:
        raise SystemExit("Missing lastAnnualDividend in company-screener (needed for dividend prefilter).")

    df = raw[["symbol", "price", exch_col, "lastAnnualDividend"]].copy()
    df = df.rename(columns={exch_col: "exchange"})
    df["ticker"] = df["symbol"].astype(str).str.upper().str.strip()
    df["exchange"] = df["exchange"].astype(str).str.upper().str.strip()
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["lastAnnualDividend"] = pd.to_numeric(df["lastAnnualDividend"], errors="coerce")

    df = df[df["exchange"].isin(ALLOWED_EXCHANGES)].copy()
    df = df.dropna(subset=["ticker", "price"])
    df = df[df["price"] > 0].copy()

    df["divYield_est"] = (df["lastAnnualDividend"] / df["price"]).apply(_normalize_div_yield)

    stage1 = df[df["divYield_est"].notna() & (df["divYield_est"] >= TREASURY_10Y)].copy()
    stage1 = stage1.sort_values("divYield_est", ascending=False).head(MAX_TICKERS_TO_EVALUATE)

    results = []
    skipped_missing = 0

    debug_saved = 0

    for _, r in stage1.iterrows():
        sym = r["ticker"]
        px = float(r["price"])
        exch = r["exchange"]

        ratios_payload = fetch_ratios_ttm(sym)
        metrics_payload = fetch_key_metrics_ttm(sym)

        # Save first few raw payloads to files so you can open them in the repo
        if debug_saved < DEBUG_SAVE_FIRST_N:
            with open(f"debug_ratios_{debug_saved+1}.json", "w") as f:
                json.dump({"symbol": sym, "payload": ratios_payload}, f, indent=2)
            with open(f"debug_metrics_{debug_saved+1}.json", "w") as f:
                json.dump({"symbol": sym, "payload": metrics_payload}, f, indent=2)
            debug_saved += 1

        ratios = _extract_first_record(ratios_payload)
        metrics = _extract_first_record(metrics_payload)

        pe = pb = debt_equity = div_yield = None

        if isinstance(ratios, dict):
            pe = _to_float(_pick(ratios, ["peRatioTTM", "peRatio", "priceToEarningsRatioTTM", "priceToEarningsRatio"]))
            pb = _to_float(_pick(ratios, ["priceToBookRatioTTM", "priceToBookRatio", "pbRatioTTM", "pbRatio"]))
            debt_equity = _to_float(_pick(ratios, ["debtEquityRatioTTM", "debtEquityRatio", "debtToEquity", "debtToEquityRatio"]))
            div_yield = _normalize_div_yield(_pick(ratios, ["dividendYieldTTM", "dividendYield"]))

        if isinstance(metrics, dict):
            if pe is None:
                pe = _to_float(_pick(metrics, ["peRatioTTM", "peRatio"]))
            if pb is None:
                pb = _to_float(_pick(metrics, ["pbRatioTTM", "pbRatio", "priceToBookRatioTTM", "priceToBookRatio"]))
            if debt_equity is None:
                debt_equity = _to_float(_pick(metrics, ["debtEquityRatioTTM", "debtEquityRatio", "debtToEquity", "debtToEquityRatio"]))
            if div_yield is None:
                div_yield = _normalize_div_yield(_pick(metrics, ["dividendYieldTTM", "dividendYield"]))

        # Dividend yield fallback: lastAnnualDividend / price
        if div_yield is None:
            div_yield = _normalize_div_yield(r["divYield_est"])

        if pe is None or pb is None or debt_equity is None or div_yield is None:
            skipped_missing += 1
            continue

        rule_div = div_yield >= TREASURY_10Y
        rule_pe = pe <= 13
        rule_pb = pb <= 1
        rule_de = debt_equity <= 1

        passes = rule_div and rule_pe and rule_pb and rule_de

        results.append({
            "ticker": sym,
            "exchange": exch,
            "price": round(px, 2),
            "divYield": round(float(div_yield), 6),
            "pe": round(float(pe), 4),
            "pb": round(float(pb), 4),
            "debtEquity": round(float(debt_equity), 4),
            "rule_div": bool(rule_div),
            "rule_pe": bool(rule_pe),
            "rule_pb": bool(rule_pb),
            "rule_de": bool(rule_de),
            "passes": bool(passes),
        })

    out = pd.DataFrame(results)

    pass_df = out[out["passes"]].copy() if not out.empty else out
    if not out.empty:
        out["rulesPassed"] = (
            out["rule_div"].astype(int) +
            out["rule_pe"].astype(int) +
            out["rule_pb"].astype(int) +
            out["rule_de"].astype(int)
        )
        top_df = out.sort_values(["rulesPassed", "divYield"], ascending=[False, False]).head(60)
    else:
        top_df = out

    stats = {
        "treasuryYield10y": TREASURY_10Y,
        "universe": int(len(df)),
        "stage1_prefilter": int(len(stage1)),
        "evaluated": int(len(out)),
        "skipped_missing": int(skipped_missing),
        "all_rules_pass": int(len(pass_df)),
        "debug_files_written": int(debug_saved),
        "notes": "Open debug_ratios_*.json and debug_metrics_*.json in the repo to see the exact stable response shape."
    }

    payload = {
        "runDate": run_date,
        "stats": stats,
        "pass": pass_df.drop(columns=["passes"], errors="ignore").to_dict(orient="records") if not pass_df.empty else [],
        "topCandidates": top_df.drop(columns=["passes"], errors="ignore").to_dict(orient="records") if not top_df.empty else [],
    }

    with open("passlist.json", "w") as f:
        json.dump(payload, f, indent=2)

    pass_df.to_csv("passlist.csv", index=False)

    print("Stats:", stats)
    print(f"Wrote passlist.json and passlist.csv with {len(pass_df)} PASS tickers")


if __name__ == "__main__":
    main()
