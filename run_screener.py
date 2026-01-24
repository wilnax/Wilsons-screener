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

def get_json(url, params, timeout=120):
    r = requests.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()

def download_csv(url, params, timeout=300):
    r = requests.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    return pd.read_csv(pd.io.common.BytesIO(r.content))

def latest_trading_day_close():
    url = f"{BASE}/SHARADAR/SEP.json"
    params = {
        "api_key": API_KEY,
        "qopts.columns": "date,ticker,close",
        "qopts.per_page": 1,
        "qopts.sort": "date desc",
    }
    j = get_json(url, params)
    cols = [c["name"] for c in j["datatable"]["columns"]]
    row = j["datatable"]["data"][0]
    d = dict(zip(cols, row))
    return d["date"]

def load_sep_closes(latest_date):
    url = f"{BASE}/SHARADAR/SEP.csv"
    params = {
        "api_key": API_KEY,
        "date": latest_date,
        "qopts.columns": "ticker,close",
        "qopts.export": "true",
    }
    df = download_csv(url, params)
    df.columns = [c.lower() for c in df.columns]
    df = df.dropna(subset=["ticker", "close"])
    return df.drop_duplicates(subset=["ticker"])

def load_sf1_latest():
    url = f"{BASE}/SHARADAR/SF1.csv"
    params = {
        "api_key": API_KEY,
        "dimension": "ART,MRQ",
        "qopts.columns": "ticker,dimension,calendardate,lastupdated,epsusd,bvps,dpsttm,debtlt,equity",
        "qopts.export": "true",
    }
    df = download_csv(url, params)
    df.columns = [c.lower() for c in df.columns]
    return df

def pick_latest_by_dim(df, dim):
    sub = df[df["dimension"] == dim].copy()
    sub["calendardate"] = pd.to_datetime(sub["calendardate"], errors="coerce")
    sub["lastupdated"] = pd.to_datetime(sub["lastupdated"], errors="coerce")
    sub = sub.sort_values(["ticker", "calendardate", "lastupdated"])
    return sub.groupby("ticker", as_index=False).tail(1)

def main():
    latest_date = latest_trading_day_close()
    closes = load_sep_closes(latest_date)
    sf1 = load_sf1_latest()

    required_cols = {"ticker", "dimension", "calendardate", "epsusd", "bvps", "debtlt", "equity"}
    missing = required_cols - set(sf1.columns)
    if missing:
        raise SystemExit(
            f"SF1 missing columns: {sorted(missing)}. "
            f"Update qopts.columns to match your SF1 schema."
        )

    art = pick_latest_by_dim(sf1, "ART")
    mrq = pick_latest_by_dim(sf1, "MRQ")

    df = closes.merge(art, on="ticker", how="inner")
    df = df.merge(mrq[["ticker", "bvps", "debtlt", "equity"]], on="ticker", how="left")

    df["pe"] = df["close"] / df["epsusd"]
    df["pb"] = df["close"] / df["bvps"]
    df["debt_equity"] = df["debtlt"] / df["equity"]

    if "dpsttm" in df.columns:
        df["div_yield"] = df["dpsttm"] / df["close"]
    else:
        df["div_yield"] = pd.NA

    for col in ["pe", "pb", "debt_equity", "div_yield"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["passes"] = (
        (df["div_yield"].notna()) &
        (df["div_yield"] >= TREASURY_10Y) &
        (df["pe"] <= 13) &
        (df["pb"] <= 1) &
        (df["debt_equity"] <= 1)
    )

    winners = df[df["passes"]].copy().sort_values("div_yield", ascending=False)

    pass_list = []
    for _, r in winners.iterrows():
        pass_list.append({
            "ticker": r["ticker"],
            "price": round(float(r["close"]), 2),
            "pe": round(float(r["pe"]), 4) if pd.notna(r["pe"]) else None,
            "pb": round(float(r["pb"]), 4) if pd.notna(r["pb"]) else None,
            "divYield": round(float(r["div_yield"]), 6) if pd.notna(r["div_yield"]) else None,
            "debtEquity": round(float(r["debt_equity"]), 4) if pd.notna(r["debt_equity"]) else None,
            "priceDate": str(latest_date),
        })

    out = {
        "runDate": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "asOfPriceDate": str(latest_date),
        "treasuryYield10y": TREASURY_10Y,
        "pass": pass_list
    }

    with open("passlist.json", "w") as f:
        json.dump(out, f, indent=2)

    print(f"Wrote passlist.json with {len(pass_list)} PASS tickers")

if __name__ == "__main__":
    main()
