"""config/sources.toml: the bronze sources and the stations they are read for.

Names are derived, never configured: a source's bronze table, raw prefix and API request all come from its
`system` and `table`. No pyspark here: the ingest Lambda imports this module.
"""

import datetime as dt
import re
import tomllib
import urllib.parse
from dataclasses import dataclass

NAME = re.compile(r"^[a-z][a-z0-9_]*$")


@dataclass(frozen=True)
class Source:
    system: str
    table: str
    api_url: str
    variables: tuple[str, ...]

    @property
    def bronze_table(self) -> str:
        """`open_meteo` + `hourly` -> `open_meteo_hourly`: bronze mirrors the source, without the layer in the name."""
        return f"{self.system}_{self.table}"


@dataclass(frozen=True)
class Station:
    station_id: str
    name: str
    latitude: float
    longitude: float


@dataclass(frozen=True)
class Config:
    sources: tuple[Source, ...]
    stations: tuple[Station, ...]

    def source(self, bronze_table: str) -> Source:
        for source in self.sources:
            if source.bronze_table == bronze_table:
                return source
        raise ValueError(f"no source {bronze_table!r} in config; known: {[s.bronze_table for s in self.sources]}")


def parse_config(text: str) -> Config:
    """Parses and validates the TOML. Raises ValueError listing every problem found."""
    data = tomllib.loads(text)
    problems = []
    sources = tuple(
        Source(s["system"], s["table"], s["api_url"], tuple(s["variables"])) for s in data.get("sources", [])
    )
    stations = tuple(
        Station(s["station_id"], s["name"], float(s["latitude"]), float(s["longitude"])) for s in data.get("stations", [])
    )
    if not sources:
        problems.append("no [[sources]]")
    if not stations:
        problems.append("no [[stations]]")
    for source in sources:
        for field, value in (("system", source.system), ("table", source.table), *(("variable", v) for v in source.variables)):
            if not NAME.match(value):
                problems.append(f"source {field} {value!r} must match {NAME.pattern}")
        if not source.variables:
            problems.append(f"source {source.bronze_table} has no variables")
    for station in stations:
        if not NAME.match(station.station_id):
            problems.append(f"station_id {station.station_id!r} must match {NAME.pattern}")
        if not (-90 <= station.latitude <= 90 and -180 <= station.longitude <= 180):
            problems.append(f"station {station.station_id} has an impossible position")
    for kind, names in (("bronze table", [s.bronze_table for s in sources]), ("station_id", [s.station_id for s in stations])):
        if duplicates := sorted({n for n in names if names.count(n) > 1}):
            problems.append(f"duplicate {kind}: {duplicates}")
    if problems:
        raise ValueError("invalid config/sources.toml:\n  " + "\n  ".join(problems))
    return Config(sources, stations)


def raw_prefix(source: Source, date: dt.date) -> str:
    """Where a day's raw files land in the raw bucket: one prefix per source and date, so a load reads exactly one day."""
    return f"{source.system}/{source.table}/date={date.isoformat()}/"


def raw_key(source: Source, date: dt.date, station: Station) -> str:
    return f"{raw_prefix(source, date)}{station.station_id}.json"


def request_url(source: Source, station: Station, date: dt.date) -> str:
    """One station-day from the Open-Meteo archive, in UTC."""
    query = urllib.parse.urlencode(
        {
            "latitude": station.latitude,
            "longitude": station.longitude,
            "start_date": date.isoformat(),
            "end_date": date.isoformat(),
            source.table: ",".join(source.variables),
            "timezone": "UTC",
        }
    )
    return f"{source.api_url}?{query}"


def run_date(requested: str | None, today: dt.date) -> dt.date:
    """The date to ingest: the requested one, or a week ago, because the archive API lags a few days behind."""
    if requested:
        return dt.date.fromisoformat(requested)
    return today - dt.timedelta(days=7)


def split_s3_uri(uri: str) -> tuple[str, str]:
    """`s3://bucket/a/b` -> (`bucket`, `a/b`)."""
    if not uri.startswith("s3://"):
        raise ValueError(f"not an s3:// URI: {uri}")
    bucket, _, key = uri[len("s3://"):].partition("/")
    return bucket, key
