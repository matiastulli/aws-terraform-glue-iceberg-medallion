"""Lambda: fetch one run of ONE source for every station, and land each API response untouched in the raw bucket at
<system>/<table>/date=YYYY-MM-DD/<station_id>.json.

Generic over sources: the event names the source (its bronze table, e.g. open_meteo_hourly), and config/sources.toml
says how to ask its API (the source's kind, medallion/config.py build_request). Each source runs as its own
source_pipeline execution, so one failing API never blocks another. Rewriting the same keys makes reruns safe.

Event: {"source": "open_meteo_hourly", "date": "YYYY-MM-DD"}. The date is optional: it defaults to today minus the
source's lag_days. Returns what the rest of the pipeline needs, including the silver job that cleans this source.
"""

import datetime as dt
import json
import os
import urllib.request
from pathlib import Path

import boto3

from medallion.config import build_request, parse_config, raw_key, run_date

CONFIG = parse_config((Path(__file__).parent / "sources.toml").read_text(encoding="utf-8"))
s3 = boto3.client("s3")


def handler(event, context):
    source = CONFIG.source(event["source"])
    day = run_date(event.get("date"), dt.datetime.now(dt.timezone.utc).date(), source.lag_days)
    bucket = os.environ["RAW_BUCKET"]
    for station in CONFIG.stations:
        url, headers = build_request(source, station, day)
        with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=30) as response:
            body = response.read()
        json.loads(body)  # fail here on a non-JSON body (an error page), not later in the load
        s3.put_object(Bucket=bucket, Key=raw_key(source, day, station), Body=body, ContentType="application/json")
    result = {"source": source.bronze_table, "date": day.isoformat(), "files": len(CONFIG.stations), "silver_job": source.silver_job}
    print(json.dumps(result))
    return result
