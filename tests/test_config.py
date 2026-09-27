import datetime as dt
from pathlib import Path

import pytest

from medallion.config import parse_config, raw_key, request_url, run_date

VALID = """
[[sources]]
system = "open_meteo"
table = "hourly"
api_url = "https://archive-api.open-meteo.com/v1/archive"
variables = ["temperature_2m", "precipitation"]

[[stations]]
station_id = "buenos_aires"
name = "Buenos Aires"
latitude = -34.61
longitude = -58.38
"""


def test_the_committed_config_is_valid():
    config = parse_config((Path(__file__).parents[1] / "config" / "sources.toml").read_text())
    assert [s.bronze_table for s in config.sources] == ["open_meteo_hourly"]


def test_names_paths_and_request_are_derived_from_system_and_table():
    config = parse_config(VALID)
    source, station = config.source("open_meteo_hourly"), config.stations[0]
    day = dt.date(2026, 9, 20)

    assert source.bronze_table == "open_meteo_hourly"
    assert raw_key(source, day, station) == "open_meteo/hourly/date=2026-09-20/buenos_aires.json"
    assert "hourly=temperature_2m%2Cprecipitation" in request_url(source, station, day)
    assert "start_date=2026-09-20&end_date=2026-09-20" in request_url(source, station, day)


@pytest.mark.parametrize(
    "change, message",
    [
        (('station_id = "buenos_aires"', 'station_id = "Buenos Aires"'), "station_id 'Buenos Aires' must match"),
        (('system = "open_meteo"', 'system = "open-meteo"'), "source system 'open-meteo' must match"),
        (("latitude = -34.61", "latitude = -134.61"), "impossible position"),
    ],
)
def test_invalid_config_is_rejected(change, message):
    with pytest.raises(ValueError, match=message):
        parse_config(VALID.replace(*change))


def test_duplicate_station_ids_are_rejected():
    # A duplicate station would write two raw files with the same key: one silently overwrites the other.
    stations = VALID[VALID.index("[[stations]]"):]
    with pytest.raises(ValueError, match=r"duplicate station_id: \['buenos_aires'\]"):
        parse_config(VALID + stations)


def test_run_date_defaults_to_a_week_ago():
    assert run_date(None, dt.date(2026, 9, 27)) == dt.date(2026, 9, 20)
    assert run_date("2026-09-01", dt.date(2026, 9, 27)) == dt.date(2026, 9, 1)
