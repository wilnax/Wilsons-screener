import os
import json
import datetime
import requests
import pandas as pd

FMP_API_KEY = os.environ.get("FMP_API_KEY")
if not FMP_API_KEY:
    raise SystemExit("Missing FMP_API_KEY (set it as a GitHub Secret)")

# 4.23% => 0.0423
TREASURY_10Y = float(os.environ.get("TREASURY_10Y", "0.0423"))

FMP_BASE = "https://financialmodelingprep.com/stable"
ALLOWED_EXCHANGES = {"NYSE", "NASDAQ", "AMEX"}


def _get_json(url: str, params: dict, timeout: int = 180):
    r = requests.get(url, params=params, timeout=timeout)
    if not r.ok:
        snippet = (r.text or "")[:1500]
        print(f"HTTP {r.status_code} error for {url}\nResponse snippet:\n{snippet}\n")
        r.raise_for_status()
    return r.json()


def _pick_existing_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def load_stock_list() -> pd.DataFrame:
    """
    Current-ish universe + prices + exchange.
    Endpoint: /stable/stock-list
    """
    url = f"{FMP_BASE}/stock-list"
    j = _get_json(url, {"apikey": FMP_API_KEY}, timeout=180)
    if not isinstance(j, list) or len(j) == 0:
        raise SystemExit("stock-list returned empty. Check your FMP plan/key.")

    df = pd.DataFrame(j)

    # Normalize common columns
    # Typical fields include: symbol, name, price, exchangeShortName, type
    sym_col = _pick_existing_col(df, ["symbol", "ticker"])
    px_col = _pick_existing_col(df, ["price", "lastPrice"])
    exch_col = _pick_existing_col(df, ["exchangeShortName", "exchange"])
    type_col = _pick_existing_col(df, ["type"])

    if not sym_col or not px_col or not exch_col:
        raise SystemExit(f"stock-list missing expected columns. Got columns: {list(df.columns)}")

    df = df.rename(columns={sym_col: "symbol", px_col: "price", exch_col: "exchange"})

    # Filter to US exchanges
    df["exchange"] = df["exchange"].astype(str).str.upper()
    df = df[df["exchange"].isin(ALLOWED_EXCHANGES)].copy()

    if type_col and type_col in df.columns:
        df[type_col] = df[type_col].astype(str).str.lower()
        df = df[df[type_col] == "stock"].copy()

    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df = df.dropna(subset=["symbol", "price"])
    df = df[df["price"] > 0].copy()

    df["symbol"] = df["symbol"].astype(str).str.upper().str.strip()
    df = df.drop_duplicates(subset=["symbol"])

    return df[["symbol", "price", "exchange"]]


def load_ratios_ttm_bulk() -> pd.DataFrame:
    """
    Ratios TTM Bulk includes valuation/leverage ratios across the universe.
    Endpoint: /stable/ratios-ttm-bulk
    """
    url = f"{FMP_BASE}/ratios-ttm-bulk"
    j = _get_json(url, {"apikey": FMP_API_KEY}, timeout=240)
    if not isinstance(j, list) or len(j) == 0:
        raise SystemExit("ratios-ttm-bulk returned empty. Your plan may not include Bulk endpoints.")

    df = pd.DataFrame(j)

    # Normalize symbol
    sym_col = _pick_existing_col(df, ["symbol", "ticker"])
    if not sym_col:
        raise SystemExit(f"ratios-ttm-bulk missing symbol column. Got columns: {list(df.columns)}")
    df = df.rename(columns={sym_col: "symbol"})
    df["symbol"] = df["symbol"].astype(str).str.upper().str.strip()

    # Try to find the needed ratios (field names vary slightly across API versions)
    pe_col = _pick_existing_col(df, [
        "priceToEarningsRatioTTM", "priceToEarningsRatio", "priceEarningsRatioTTM",
        "peRatioTTM", "peRatio", "pe"
    ])
    pb_col = _pick_existing_col(df, [
        "priceToBookRatioTTM", "priceToBookRatio", "pbRatioTTM", "pbRatio", "pb"
    ])
    dy_col = _pick_existing_col(df, [
        "dividendYieldTTM", "dividendYield", "divYield", "divYieldTTM"
    ])
    de_col = _pick_existing_col(df, [
        "debtEquityRatioTTM", "debtEquityRatio", "debtToEquity", "debtToEquityRatio"
    ])

    missing = [name for name, col in [
        ("PE", pe_col), ("PB", pb_col), ("DividendYield", dy_col), ("DebtEquity", de_col)
    ] if col is None]

    if missing:
        print("Ratios columns returned:", list(df.columns))
        raise SystemExit(f"Could not find required ratio columns: {missing}")

    df = df.rename(columns={
        pe_col: "pe",
        pb_col: "pb",
        dy_col: "divYield",
        de_col: "debtEquity",
    })

    for c in ["pe", "pb", "divYield", "debtEquity"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.drop_duplicates(subset=["symbol"])
    return df[["symbol", "pe", "pb", "divYield", "debtEquity"]]


def main() -> None:
    prices = load_stock_list()
    ratios = load_ratios_ttm_bulk()

    df = prices.merge(ratios, on="symbol", how="inner")

    # Rules (Wilson's Algorithm)
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
            "ticker": r["symbol"],
            "exchange": r["exchange"],
            "price": round(float(r["price"]), 2),
            "divYield": None if pd.isna(r["divYield"]) else round(float(r["divYield"]), 6),
            "pe": None if pd.isna(r["pe"]) else round(float(r["pe"]), 4),
            "pb": None if pd.isna(r["pb"]) else round(float(r["pb"]), 4),
            "debtEquity": None if pd.isna(r["debtEquity"]) else round(float(r["debtEquity"]), 4),
        })

    top = df.sort_values(["rulesPassed", "divYield"], ascending=[False, False]).head(60)
    top_candidates = []
    for _, r in top.iterrows():
        top_candidates.append({
            "ticker": r["symbol"],
            "exchange": r["exchange"],
            "rulesPassed": int(r["rulesPassed"]),
            "price": round(float(r["price"]), 2),
            "divYield": None if pd.isna(r["divYield"]) else round(float(r["divYield"]), 6),
            "pe": None if pd.isna(r["pe"]) else round(float(r["pe"]), 4),
            "pb": None if pd.isna(r["pb"]) else round(float(r["pb"]), 4),
            "debtEquity": None if pd.isna(r["debtEquity"]) else round(float(r["debtEquity"]), 4),
        })

    stats = {
        "treasuryYield10y": TREASURY_10Y,
        "priceUniverseRows": int(len(prices)),
        "ratiosRows": int(len(ratios)),
        "mergedRows": int(len(df)),
        "div_rule_pass": int(df["rule_div"].sum()),
        "pe_rule_pass": int(df["rule_pe"].sum()),
        "pb_rule_pass": int(df["rule_pb"].sum()),
        "de_rule_pass": int(df["rule_de"].sum()),
        "all_rules_pass": int(df["passes"].sum()),
        "exchangesIncluded": sorted(list(ALLOWED_EXCHANGES)),
    }

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
