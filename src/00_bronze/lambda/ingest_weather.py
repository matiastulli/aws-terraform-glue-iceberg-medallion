"""Lambda: fetch one day of readings for every source and station in config/sources.toml, and land the API responses
untouched in the raw bucket at <system>/<table>/date=YYYY-MM-DD/<station_id>.json.

Rewriting the same key for the same day makes reruns safe. Returns what the load step needs.
Event: {"date": "YYYY-MM-DD"} (optional; defaults to a week ago, since the archive API lags a few days).
"""

import datetime as dt
import json
import os
import urllib.request
from pathlib import Path

import boto3

from medallion.config import parse_config, raw_key, request_url, run_date

CONFIG = parse_config((Path(__file__).parent / "sources.toml").read_text(encoding="utf-8"))
s3 = boto3.client("s3")


def handler(event, context):
    day = run_date((event or {}).get("date"), dt.datetime.now(dt.timezone.utc).date())
    bucket = os.environ["RAW_BUCKET"]
    files = 0
    for source in CONFIG.sources:
        for station in CONFIG.stations:
            request = urllib.request.Request(request_url(source, station, day), headers={"User-Agent": "weather-lakehouse"})
            with urllib.request.urlopen(request, timeout=20) as response:
                body = response.read()
            json.loads(body)  # fail here on a non-JSON body, not later in the load
            s3.put_object(Bucket=bucket, Key=raw_key(source, day, station), Body=body, ContentType="application/json")
            files += 1
    result = {"date": day.isoformat(), "sources": [s.bronze_table for s in CONFIG.sources], "files": files}
    print(json.dumps(result))
    return result
