import os
import json
import datetime
import requests
import pandas as pd

API_KEY = os.environ.get("NASDAQ_API_KEY")
if not API_KEY:
    raise SystemExit("Missing NASDAQ_API_KEY")

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


def pick_best_row_per_ticker(df: pd.DataFrame, required_cols: list[str]) -> pd.DataFrame:
    """
    Pick one row per ticker:
    - Prefer rows where all required_cols are non-null
    - Within that, prefer most recent (lastupdated, calendardate)
    """
    df = df.copy()
    df["calendardate"] = pd.to_datetime(df["calendardate"], errors="coerce")
    df["lastupdated"] = pd.to_datetime(df["lastupdated"], errors="coerce")

    # Has all required numeric fields?
    has_all = pd.Series(True, index=df.index)
    for c in required_cols:
        has_all &= df[c].notna()
    df["has_all_required"] = has_all.astype(int)

    # Sort so best rows are last per ticker
    df = df.sort_values(["ticker", "has_all_required", "lastupdated", "calendardate"])

    # Take best (last) row per ticker
    best = df.groupby("ticker", as_index=False).tail(1)

    # Ensure ticker is a normal column
    if "ticker" not in best.columns:
        best = best.reset_index()
    return best.reset_index(drop=True)


def main() -> None:
    cols = "ticker,dimension,calendardate,lastupdated,price,pe,pb,divyield,debtnc,equity"
    sf1 = fetch_datatable_all_rows(
        "SHARADAR/SF1",
        params={
            "dimension": "ART,MRQ,MRY,TTM",
            "qopts.columns": cols,
            "qopts.per_page": 10000,
        },
        timeout=180,
        max_pages=500,
    )

    required = {"ticker", "dimension", "calendardate", "lastupdated", "price", "pe", "pb", "divyield", "debtnc", "equity"}
    missing = required - set(sf1.columns)
    if missing:
        raise SystemExit(f"SF1 missing expected columns: {sorted(missing)}")

    # numeric coercion
    for c in ["price", "pe", "pb", "divyield", "debtnc", "equity"]:
        sf1[c] = pd.to_numeric(sf1[c], errors="coerce")

    # Normalize dividend yield if it looks like percent
    sf1.loc[sf1["divyield"] > 1, "divyield"] = sf1.loc[sf1["divyield"] > 1, "divyield"] / 100.0

    latest = pick_best_row_per_ticker(sf1, required_cols=["price", "pe", "pb", "divyield", "debtnc", "equity"])

    # Long-term debt to equity ratio (your screener)
    latest["lt_debt_equity"] = latest["debtnc"] / latest["equity"]
    latest["lt_debt_equity"] = pd.to_numeric(latest["lt_debt_equity"], errors="coerce")

    # Rule flags
    latest["rule_div"] = latest["divyield"].notna() & (latest["divyield"] >= TREASURY_10Y)
    latest["rule_pe"]  = latest["pe"].notna() & (latest["pe"] <= 13)
    latest["rule_pb"]  = latest["pb"].notna() & (latest["pb"] <= 1)
    latest["rule_ltde"] = latest["lt_debt_equity"].notna() & (latest["lt_debt_equity"] <= 1)
    latest["passes"] = latest["rule_div"] & latest["rule_pe"] & latest["rule_pb"] & latest["rule_ltde"]

    winners = latest[latest["passes"]].copy().sort_values("divyield", ascending=False)

    pass_list = []
    for _, r in winners.iterrows():
        cd = r["calendardate"]
        pass_list.append({
            "ticker": r["ticker"],
            "dimension": r["dimension"],
            "price": None if pd.isna(r["price"]) else round(float(r["price"]), 2),
            "pe": None if pd.isna(r["pe"]) else round(float(r["pe"]), 4),
            "pb": None if pd.isna(r["pb"]) else round(float(r["pb"]), 4),
            "divYield": None if pd.isna(r["divyield"]) else round(float(r["divyield"]), 6),
            "ltDebtEquity": None if pd.isna(r["lt_debt_equity"]) else round(float(r["lt_debt_equity"]), 4),
            "priceDate": None if pd.isna(cd) else str(cd.date()),
        })

    diag = {
        "rowsPulled": int(len(sf1)),
        "tickersEvaluated": int(latest["ticker"].nunique()) if "ticker" in latest.columns else int(len(latest)),
        "maxCalendardate": str(latest["calendardate"].max().date()) if latest["calendardate"].notna().any() else None,
        "nonnull_price": int(latest["price"].notna().sum()),
        "nonnull_pe": int(latest["pe"].notna().sum()),
        "nonnull_pb": int(latest["pb"].notna().sum()),
        "nonnull_divyield": int(latest["divyield"].notna().sum()),
        "nonnull_debtnc": int(latest["debtnc"].notna().sum()),
        "nonnull_equity": int(latest["equity"].notna().sum()),
        "nonnull_ltde": int(latest["lt_debt_equity"].notna().sum()),
        "rule_div_pass": int(latest["rule_div"].sum()),
        "rule_pe_pass": int(latest["rule_pe"].sum()),
        "rule_pb_pass": int(latest["rule_pb"].sum()),
        "rule_ltde_pass": int(latest["rule_ltde"].sum()),
        "all_rules_pass": int(latest["passes"].sum()),
    }

    sample = latest.sort_values("divyield", ascending=False).head(25)
    sample_list = []
    for _, r in sample.iterrows():
        cd = r["calendardate"]
        sample_list.append({
            "ticker": r["ticker"],
            "dimension": r["dimension"],
            "price": None if pd.isna(r["price"]) else round(float(r["price"]), 2),
            "pe": None if pd.isna(r["pe"]) else round(float(r["pe"]), 4),
            "pb": None if pd.isna(r["pb"]) else round(float(r["pb"]), 4),
            "divYield": None if pd.isna(r["divyield"]) else round(float(r["divyield"]), 6),
            "ltDebtEquity": None if pd.isna(r["lt_debt_equity"]) else round(float(r["lt_debt_equity"]), 4),
            "priceDate": None if pd.isna(cd) else str(cd.date()),
        })

    out = {
        "runDate": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "treasuryYield10y": TREASURY_10Y,
        "diagnostics": diag,
        "pass": pass_list,
        "sampleTop25ByYield": sample_list,
    }

    with open("passlist.json", "w") as f:
        json.dump(out, f, indent=2)

    print("Diagnostics:", diag)
    print(f"Wrote passlist.json with {len(pass_list)} PASS tickers")


if __name__ == "__main__":
    main()
