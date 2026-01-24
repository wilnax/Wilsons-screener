import os
import json
import datetime

API_KEY = os.environ.get("NASDAQ_API_KEY")
if not API_KEY:
    raise SystemExit("Missing NASDAQ_API_KEY")

out = {
    "runDate": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    "status": "Pipeline OK — Sharadar SF1 hookup next",
    "pass": []
}

with open("passlist.json", "w") as f:
    json.dump(out, f, indent=2)

print("passlist.json written successfully")
