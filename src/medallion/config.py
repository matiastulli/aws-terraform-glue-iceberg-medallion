"""config/sources.toml: the bronze sources, the streams, and the stations they are read for.

Names are derived, never configured: a source's bronze table and raw prefix come from its `system` and `table`, and
its `kind` says how to ask the API for one station (build_request). No pyspark here: the ingest Lambda imports this.
"""

import datetime as dt
import re
import tomllib
import urllib.parse
from dataclasses import dataclass

NAME = re.compile(r"^[a-z][a-z0-9_]*$")
WIKIDATA_ID = re.compile(r"^Q[1-9][0-9]*$")
KINDS = ("open_meteo_archive", "wikidata_sparql")
# Wikidata's policy asks clients for a descriptive User-Agent; anonymous ones get throttled or blocked.
USER_AGENT = "weather-lakehouse/0.1 (https://github.com/matiastulli/aws-terraform-glue-iceberg-medallion)"

# Population (P1082) statements with their point in time (P585, optional on Wikidata) and rank. The variable names are
# the Wikidata property names, so bronze mirrors the source; silver renames them.
WIKIDATA_POPULATION_QUERY = """SELECT ?population ?point_in_time ?rank WHERE {{
  wd:{wikidata_id} p:P1082 ?statement .
  ?statement ps:P1082 ?population ;
             wikibase:rank ?rank .
  OPTIONAL {{ ?statement pq:P585 ?point_in_time }}
}}
ORDER BY ?point_in_time"""


@dataclass(frozen=True)
class Source:
    system: str
    table: str
    kind: str
    api_url: str
    silver_job: str  # the Glue job that cleans this source's bronze batch into silver
    lag_days: int  # how far behind today the source publishes: the default run date is today - lag_days
    variables: tuple[str, ...] = ()  # open_meteo_archive only

    @property
    def bronze_table(self) -> str:
        """`open_meteo` + `hourly` -> `open_meteo_hourly`: bronze mirrors the source, without the layer in the name."""
        return f"{self.system}_{self.table}"


@dataclass(frozen=True)
class Stream:
    """A live source: messages arrive on a queue and are appended to bronze as they come (docs/PLAN.md step 6)."""

    system: str
    table: str
    duplicate_rate: float  # simulator: share of readings sent twice, like a sensor retrying
    late_rate: float  # simulator: share of readings sent late, like a sensor that buffered while offline
    max_late_minutes: int

    @property
    def bronze_table(self) -> str:
        return f"{self.system}_{self.table}"


@dataclass(frozen=True)
class Station:
    station_id: str
    name: str
    latitude: float
    longitude: float
    wikidata_id: str | None = None


@dataclass(frozen=True)
class Config:
    sources: tuple[Source, ...]
    stations: tuple[Station, ...]
    streams: tuple[Stream, ...] = ()

    def source(self, bronze_table: str) -> Source:
        for source in self.sources:
            if source.bronze_table == bronze_table:
                return source
        raise ValueError(f"no source {bronze_table!r} in config; known: {[s.bronze_table for s in self.sources]}")

    def stream(self, bronze_table: str) -> Stream:
        for stream in self.streams:
            if stream.bronze_table == bronze_table:
                return stream
        raise ValueError(f"no stream {bronze_table!r} in config; known: {[s.bronze_table for s in self.streams]}")


def parse_config(text: str) -> Config:
    """Parses and validates the TOML. Raises ValueError listing every problem found."""
    data = tomllib.loads(text)
    problems = []
    sources = tuple(
        Source(s["system"], s["table"], s["kind"], s["api_url"], s["silver_job"], int(s["lag_days"]), tuple(s.get("variables", [])))
        for s in data.get("sources", [])
    )
    streams = tuple(
        Stream(s["system"], s["table"], float(s["duplicate_rate"]), float(s["late_rate"]), int(s["max_late_minutes"]))
        for s in data.get("streams", [])
    )
    stations = tuple(
        Station(s["station_id"], s["name"], float(s["latitude"]), float(s["longitude"]), s.get("wikidata_id"))
        for s in data.get("stations", [])
    )
    if not sources:
        problems.append("no [[sources]]")
    if not stations:
        problems.append("no [[stations]]")
    for source in sources:
        if source.lag_days < 0:
            problems.append(f"source {source.bronze_table} has a negative lag_days")
        for field, value in (("system", source.system), ("table", source.table), ("silver_job", source.silver_job), *(("variable", v) for v in source.variables)):
            if not NAME.match(value):
                problems.append(f"source {field} {value!r} must match {NAME.pattern}")
        if source.kind not in KINDS:
            problems.append(f"source {source.bronze_table} has kind {source.kind!r}; known kinds: {list(KINDS)}")
        if source.kind == "open_meteo_archive" and not source.variables:
            problems.append(f"source {source.bronze_table} has no variables")
        if source.kind == "wikidata_sparql":
            for station in stations:
                if not WIKIDATA_ID.match(station.wikidata_id or ""):
                    problems.append(f"station {station.station_id} needs a wikidata_id like Q1486 for source {source.bronze_table}")
    for stream in streams:
        for field, value in (("system", stream.system), ("table", stream.table)):
            if not NAME.match(value):
                problems.append(f"stream {field} {value!r} must match {NAME.pattern}")
        if not (0 <= stream.duplicate_rate <= 1 and 0 <= stream.late_rate <= 1):
            problems.append(f"stream {stream.bronze_table} rates must be between 0 and 1")
        if stream.max_late_minutes < 1:
            problems.append(f"stream {stream.bronze_table} max_late_minutes must be at least 1")
    for station in stations:
        if not NAME.match(station.station_id):
            problems.append(f"station_id {station.station_id!r} must match {NAME.pattern}")
        if not (-90 <= station.latitude <= 90 and -180 <= station.longitude <= 180):
            problems.append(f"station {station.station_id} has an impossible position")
    for kind, names in (("bronze table", [s.bronze_table for s in (*sources, *streams)]), ("station_id", [s.station_id for s in stations])):
        if duplicates := sorted({n for n in names if names.count(n) > 1}):
            problems.append(f"duplicate {kind}: {duplicates}")
    if problems:
        raise ValueError("invalid config/sources.toml:\n  " + "\n  ".join(problems))
    return Config(sources, stations, streams)


def raw_prefix(source: Source, date: dt.date) -> str:
    """Where a day's raw files land in the raw bucket: one prefix per source and date, so a load reads exactly one day."""
    return f"{source.system}/{source.table}/date={date.isoformat()}/"


def raw_key(source: Source, date: dt.date, station: Station) -> str:
    return f"{raw_prefix(source, date)}{station.station_id}.json"


def build_request(source: Source, station: Station, date: dt.date) -> tuple[str, dict[str, str]]:
    """The GET request (URL, headers) that fetches one station's data for one run date."""
    headers = {"User-Agent": USER_AGENT}
    if source.kind == "open_meteo_archive":
        # One station-day from the archive, in UTC. `table` (hourly) is both the parameter and the response block.
        query = {
            "latitude": station.latitude,
            "longitude": station.longitude,
            "start_date": date.isoformat(),
            "end_date": date.isoformat(),
            source.table: ",".join(source.variables),
            "timezone": "UTC",
        }
    elif source.kind == "wikidata_sparql":
        # Every population statement Wikidata has for the station's city, as it stands today: the run date only decides
        # where the response lands in raw, so bronze keeps what Wikidata said on each run.
        query = {"query": WIKIDATA_POPULATION_QUERY.format(wikidata_id=station.wikidata_id), "format": "json"}
        headers["Accept"] = "application/sparql-results+json"
    else:
        raise ValueError(f"unknown source kind {source.kind!r}")
    return f"{source.api_url}?{urllib.parse.urlencode(query)}", headers


def run_date(requested: str | None, today: dt.date, lag_days: int) -> dt.date:
    """The date to ingest: the requested one, or today minus the source's publishing lag."""
    if requested:
        return dt.date.fromisoformat(requested)
    return today - dt.timedelta(days=lag_days)


def split_s3_uri(uri: str) -> tuple[str, str]:
    """`s3://bucket/a/b` -> (`bucket`, `a/b`)."""
    if not uri.startswith("s3://"):
        raise ValueError(f"not an s3:// URI: {uri}")
    bucket, _, key = uri[len("s3://"):].partition("/")
    return bucket, key
