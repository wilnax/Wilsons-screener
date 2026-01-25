import os
import requests
import pandas as pd

API_KEY = os.environ.get("NASDAQ_API_KEY")
if not API_KEY:
    raise SystemExit("Missing NASDAQ_API_KEY")

BASE = "https://data.nasdaq.com/api/v3/datatables"


def _raise_with_context(resp: requests.Response) -> None:
    if resp.ok:
        return
    snippet = (resp.text or "")[:2000]
    print(f"HTTP {resp.status_code} error. Response snippet:\n{snippet}\n")
    resp.raise_for_status()


def get_json(url: str, params: dict, timeout: int = 180) -> dict:
    resp = requests.get(url, params=params, timeout=timeout)
    _raise_with_context(resp)
    return resp.json()


def main() -> None:
    url = f"{BASE}/SHARADAR/SF1.json"
    params = {
        "api_key": API_KEY,
        "dimension": "ART",
        "qopts.columns": "ticker,calendardate,epsusd",
        "qopts.per_page": 5,
    }

    j = get_json(url, params, timeout=180)

    cols = [c["name"] for c in j.get("datatable", {}).get("columns", [])]
    data = j.get("datatable", {}).get("data", [])

    print("SF1 columns returned:", cols)
    print("SF1 rows returned:", len(data))
    print("SF1 first rows:", data)

    if len(cols) and len(data):
        df = pd.DataFrame(data, columns=[c.lower() for c in cols])
        print("\nDataFrame head:\n", df.head())


if __name__ == "__main__":
    main()
