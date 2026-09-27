import datetime as dt
from pathlib import Path

import pytest

from medallion.config import build_request, parse_config, raw_key, run_date

VALID = """
[[sources]]
system = "open_meteo"
table = "hourly"
kind = "open_meteo_archive"
api_url = "https://archive-api.open-meteo.com/v1/archive"
silver_job = "clean_readings"
lag_days = 7
variables = ["temperature_2m", "precipitation"]

[[stations]]
station_id = "buenos_aires"
name = "Buenos Aires"
latitude = -34.61
longitude = -58.38
wikidata_id = "Q1486"
"""

WIKIDATA_SOURCE = """
[[sources]]
system = "wikidata"
table = "population"
kind = "wikidata_sparql"
api_url = "https://query.wikidata.org/sparql"
silver_job = "clean_populations"
lag_days = 0
"""


def test_the_committed_config_is_valid():
    config = parse_config((Path(__file__).parents[1] / "config" / "sources.toml").read_text())
    assert [s.bronze_table for s in config.sources] == ["open_meteo_hourly", "wikidata_population"]


def test_names_paths_and_request_are_derived_from_system_and_table():
    config = parse_config(VALID)
    source, station = config.source("open_meteo_hourly"), config.stations[0]
    day = dt.date(2026, 9, 20)

    assert source.bronze_table == "open_meteo_hourly"
    assert raw_key(source, day, station) == "open_meteo/hourly/date=2026-09-20/buenos_aires.json"
    url, headers = build_request(source, station, day)
    assert "hourly=temperature_2m%2Cprecipitation" in url
    assert "start_date=2026-09-20&end_date=2026-09-20" in url
    assert headers["User-Agent"].startswith("weather-lakehouse/")


def test_a_wikidata_source_asks_for_the_stations_population_statements():
    config = parse_config(VALID + WIKIDATA_SOURCE)
    source = config.source("wikidata_population")

    url, headers = build_request(source, config.stations[0], dt.date(2026, 9, 20))

    assert source.bronze_table == "wikidata_population"
    assert raw_key(source, dt.date(2026, 9, 20), config.stations[0]) == "wikidata/population/date=2026-09-20/buenos_aires.json"
    assert url.startswith("https://query.wikidata.org/sparql?query=") and "wd%3AQ1486+p%3AP1082" in url
    assert headers["Accept"] == "application/sparql-results+json"


def test_a_wikidata_source_needs_a_wikidata_id_on_every_station():
    with pytest.raises(ValueError, match="station buenos_aires needs a wikidata_id"):
        parse_config(VALID.replace('wikidata_id = "Q1486"', 'wikidata_id = "1486"') + WIKIDATA_SOURCE)


@pytest.mark.parametrize(
    "change, message",
    [
        (('station_id = "buenos_aires"', 'station_id = "Buenos Aires"'), "station_id 'Buenos Aires' must match"),
        (('system = "open_meteo"', 'system = "open-meteo"'), "source system 'open-meteo' must match"),
        (("latitude = -34.61", "latitude = -134.61"), "impossible position"),
        (('kind = "open_meteo_archive"', 'kind = "csv"'), "has kind 'csv'"),
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


def test_the_default_run_date_follows_each_sources_publishing_lag():
    today = dt.date(2026, 9, 27)
    assert run_date(None, today, lag_days=7) == dt.date(2026, 9, 20)
    assert run_date(None, today, lag_days=0) == today
    assert run_date("2026-09-01", today, lag_days=7) == dt.date(2026, 9, 1)
