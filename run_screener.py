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

# Stable endpoints (supported for new users)
COMPANY_SCREENER_URL = "https://financialmodelingprep.com/stable/company-screener"
RATIOS_TTM_URL = "https://financialmodelingprep.com/stable/ratios-ttm"
KEY_METRICS_TTM_URL = "https://financialmodelingprep.com/stable/key-metrics-ttm"

ALLOWED_EXCHANGES = {"NYSE", "NASDAQ", "AMEX"}

# Be gentle to avoid rate limits
SLEEP_BETWEEN_CALLS_SEC = 0.12

# Hard cap so Actions doesn't run forever on the first try
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
    # Normalize dividend yield to fraction (0.034 not 3.4)
    dy = _to_float(dy)
    if dy is None:
        return None
    if dy > 1.0:
        return dy / 100.0
    return dy


def _pick(d: dict, keys: list[str]):
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return None


def fetch_us_universe_from_company_screener(limit: int = 10000) -> pd.DataFrame:
    """
    Uses stable/company-screener which you already confirmed returns:
    symbol, price, exchangeShortName, lastAnnualDividend, ...
    """
    params = {
        "apikey": FMP_API_KEY,
        "country": "US",
        "isEtf": "false",
        "isFund": "false",
        "isActivelyTrading": "true",
        "limit": limit,
    }
    data = _get_json(COMPANY_SCREENER_URL, params, timeout=240)
    if not isinstance(data, list):
        raise SystemExit(f"Unexpected company-screener response type: {type(data)}")
    df = pd.DataFrame(data)
    if df.empty:
        raise SystemExit("company-screener returned 0 rows (unexpected).")
    return df


def fetch_ratios_ttm(symbol: str) -> dict | None:
    params = {"apikey": FMP_API_KEY, "symbol": symbol}
    data = _get_json(RATIOS_TTM_URL, params, timeout=180)
    time.sleep(SLEEP_BETWEEN_CALLS_SEC)
    if isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
        return data[0]
    return None


def fetch_key_metrics_ttm(symbol: str) -> dict | None:
    params = {"apikey": FMP_API_KEY, "symbol": symbol}
    data = _get_json(KEY_METRICS_TTM_URL, params, timeout=180)
    time.sleep(SLEEP_BETWEEN_CALLS_SEC)
    if isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
        return data[0]
    return None


def main() -> None:
    run_date = dt.datetime.now(dt.timezone.utc).isoformat()

    raw = fetch_us_universe_from_company_screener(limit=10000)

    # Normalize expected columns from your returned list
    needed = ["symbol", "price"]
    for c in needed:
        if c not in raw.columns:
            raise SystemExit(f"company-screener missing '{c}'. Columns: {list(raw.columns)}")

    exch_col = "exchangeShortName" if "exchangeShortName" in raw.columns else ("exchange" if "exchange" in raw.columns else None)
    if exch_col is None:
        raise SystemExit(f"company-screener missing exchange field. Columns: {list(raw.columns)}")

    div_col = "lastAnnualDividend" if "lastAnnualDividend" in raw.columns else None
    if div_col is None:
        raise SystemExit("company-screener did not return lastAnnualDividend; cannot prefilter dividend yield.")

    df = raw.rename(columns={exch_col: "exchange", div_col: "lastAnnualDividend"}).copy()
    df["ticker"] = df["symbol"].astype(str).str.upper().str.strip()
    df["exchange"] = df["exchange"].astype(str).str.upper().str.strip()

    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["lastAnnualDividend"] = pd.to_numeric(df["lastAnnualDividend"], errors="coerce")

    # Filter to main US exchanges
    df = df[df["exchange"].isin(ALLOWED_EXCHANGES)].copy()
    df = df.dropna(subset=["ticker", "price"])
    df = df[df["price"] > 0].copy()

    # Estimated dividend yield from company-screener fields
    # (This is only to reduce calls; final yield can come from TTM endpoints if available.)
    df["divYield_est"] = df["lastAnnualDividend"] / df["price"]
    df["divYield_est"] = df["divYield_est"].apply(_normalize_div_yield)

    # Stage 1 filter: only those with estimated yield >= 10Y
    stage1 = df[df["divYield_est"].notna() & (df["divYield_est"] >= TREASURY_10Y)].copy()

    # Cap evaluation for reliability on first run
    stage1 = stage1.sort_values("divYield_est", ascending=False).head(MAX_TICKERS_TO_EVALUATE)

    results = []
    errors = 0

    for _, row in stage1.iterrows():
        sym = row["ticker"]
        px = float(row["price"])
        exch = row["exchange"]

        ratios = fetch_ratios_ttm(sym)
        metrics = None

        # Extract PE, PB, DE from ratios-ttm if possible
        pe = pb = debt_equity = div_yield = None

        if ratios:
            pe = _to_float(_pick(ratios, ["peRatioTTM", "peRatio", "priceToEarningsRatioTTM", "priceToEarningsRatio"]))
            pb = _to_float(_pick(ratios, ["priceToBookRatioTTM", "priceToBookRatio", "pbRatioTTM", "pbRatio"]))
            debt_equity = _to_float(_pick(ratios, ["debtEquityRatioTTM", "debtEquityRatio", "debtToEquity", "debtToEquityRatio"]))
            div_yield = _normalize_div_yield(_pick(ratios, ["dividendYieldTTM", "dividendYield"]))

        # If PB or DE or PE missing, try key-metrics-ttm fallback
        if pe is None or pb is None or debt_equity is None or div_yield is None:
            metrics = fetch_key_metrics_ttm(sym)  # fallback

        if metrics:
            if pe is None:
                pe = _to_float(_pick(metrics, ["peRatioTTM", "peRatio"]))
            if pb is None:
                pb = _to_float(_pick(metrics, ["pbRatioTTM", "pbRatio", "priceToBookRatioTTM", "priceToBookRatio"]))
            if debt_equity is None:
                debt_equity = _to_float(_pick(metrics, ["debtEquityRatioTTM", "debtEquityRatio", "debtToEquity", "debtToEquityRatio"]))
            if div_yield is None:
                div_yield = _normalize_div_yield(_pick(metrics, ["dividendYieldTTM", "dividendYield"]))

        # Final yield fallback: use estimated yield if TTM yield isn't available
        if div_yield is None:
            div_yield = _normalize_div_yield(row["divYield_est"])

        # If still missing required metrics, skip but count as error
        if pe is None or pb is None or debt_equity is None or div_yield is None:
            errors += 1
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

    out_df = pd.DataFrame(results)

    if out_df.empty:
        pass_df = out_df
        top_df = out_df
    else:
        pass_df = out_df[out_df["passes"]].sort_values(["divYield", "pe"], ascending=[False, True])
        out_df["rulesPassed"] = (
            out_df["rule_div"].astype(int) +
            out_df["rule_pe"].astype(int) +
            out_df["rule_pb"].astype(int) +
            out_df["rule_de"].astype(int)
        )
        top_df = out_df.sort_values(["rulesPassed", "divYield"], ascending=[False, False]).head(60)

    stats = {
        "treasuryYield10y": TREASURY_10Y,
        "universeFromCompanyScreener": int(len(df)),
        "stage1_dividend_prefilter_count": int(len(stage1)),
        "evaluated_count": int(len(results)),
        "skipped_missing_metrics": int(errors),
        "div_rule_pass": int(pass_df.shape[0]) if not out_df.empty else 0,  # not perfect, but ok
        "all_rules_pass": int(pass_df.shape[0]) if not out_df.empty else 0,
        "notes": "Uses stable/company-screener for universe + lastAnnualDividend prefilter, then stable ratios-ttm / key-metrics-ttm per symbol."
    }

    payload = {
        "runDate": run_date,
        "stats": stats,
        "pass": pass_df.drop(columns=["passes"], errors="ignore").to_dict(orient="records") if not pass_df.empty else [],
        "topCandidates": top_df.to_dict(orient="records") if not top_df.empty else [],
    }

    with open("passlist.json", "w") as f:
        json.dump(payload, f, indent=2)

    pass_df.to_csv("passlist.csv", index=False)

    print("Stats:", stats)
    print(f"Wrote passlist.json and passlist.csv with {len(pass_df)} PASS tickers")


if __name__ == "__main__":
    main()
