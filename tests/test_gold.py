import datetime as dt

import pytest
from pyspark.sql import functions as F

from medallion.gold import add_trends, build_agg_readings_daily, daily_readings, differs_from, with_population
from medallion.quality import DataQualityError, gold_checks, raise_if_any_failed

READINGS = "station_id string, observed_at timestamp, temperature_c double, relative_humidity_pct int, precipitation_mm double, wind_speed_kmh double"
POPULATIONS = "station_id string, reference_date date, population bigint"


def hourly(station, day, temps, rain=0.0):
    start = dt.datetime.combine(day, dt.time())
    return [(station, start + dt.timedelta(hours=h), t, 60, rain, 10.0) for h, t in enumerate(temps)]


def readings(spark, *rows):
    # Session time zone is UTC; build timestamps from UTC strings so the laptop's time zone can't shift the day.
    df = spark.createDataFrame([(s, t.strftime("%Y-%m-%d %H:%M:%S"), *rest) for s, t, *rest in [r for rs in rows for r in rs]],
                               READINGS.replace("observed_at timestamp", "observed_at string"))
    return df.withColumn("observed_at", F.to_timestamp("observed_at"))


def by_day(df, *columns):
    return {r.day: tuple(r[c] for c in columns) for r in df.withColumn("day", F.date_format("reading_date", "yyyy-MM-dd")).collect()}


D1, D2, D4 = dt.date(2026, 9, 20), dt.date(2026, 9, 21), dt.date(2026, 9, 23)


def test_hourly_readings_become_one_row_per_station_day(spark):
    daily = daily_readings(readings(spark, hourly("ushuaia", D1, [0.0, 2.0, 4.0], rain=0.5)))

    assert by_day(daily, "hours_observed", "is_complete_day", "temperature_min_c", "temperature_avg_c", "temperature_max_c", "precipitation_total_mm") == {
        "2026-09-20": (3, False, 0.0, 2.0, 4.0, 1.5),
    }


def test_trends_follow_the_calendar_not_the_row_order(spark):
    # 2026-09-22 is missing: the 23rd has no "yesterday", and its 7-day window still spans the 20th and 21st.
    daily = add_trends(daily_readings(readings(spark, hourly("s", D1, [10.0]), hourly("s", D2, [12.0]), hourly("s", D4, [20.0]))))

    assert by_day(daily, "temperature_change_c", "temperature_avg_7d_c") == {
        "2026-09-20": (None, 10.0),
        "2026-09-21": (2.0, 11.0),
        "2026-09-23": (None, 14.0),
    }


def test_each_day_gets_the_latest_population_on_or_before_it_never_a_later_one(spark):
    daily = daily_readings(readings(spark, hourly("s", dt.date(2015, 6, 1), [10.0]), hourly("s", D1, [10.0]), hourly("s", dt.date(2005, 1, 1), [10.0])))
    counts = spark.createDataFrame([("s", dt.date(2010, 1, 1), 56956), ("s", dt.date(2022, 1, 1), 82615), ("other", dt.date(2000, 1, 1), 1)], POPULATIONS)

    joined = with_population(daily, counts)

    assert by_day(joined, "population") == {
        "2005-01-01": (None,),  # before any count for this station: left for the quality checks to reject
        "2015-06-01": (56956,),  # the 2010 census, not the 2022 one
        "2026-09-20": (82615,),
    }


def gold_and_silver(spark, populations=(("s", dt.date(2022, 1, 1), 82615),)):
    silver = readings(spark, hourly("s", D1, [10.0, 12.0], rain=1.0), hourly("s", D2, [11.0], rain=2.0))
    return build_agg_readings_daily(silver, spark.createDataFrame(list(populations), POPULATIONS)), silver


def failed(checks):
    return [c.name for c in checks if not c.passed]


def test_consistent_gold_passes_every_check(spark):
    gold, silver = gold_and_silver(spark)

    assert failed(gold_checks(gold, silver)) == []


def test_a_day_without_a_population_count_fails_the_checks(spark):
    gold, silver = gold_and_silver(spark, populations=(("s", dt.date(2030, 1, 1), 99),))

    checks = gold_checks(gold, silver)

    assert failed(checks) == ["population_found"]
    with pytest.raises(DataQualityError, match="population_found: 2 station-days without a population"):
        raise_if_any_failed(checks)


def test_gold_that_lost_or_duplicated_readings_fails_reconciliation(spark):
    gold, silver = gold_and_silver(spark)

    assert failed(gold_checks(gold.where(F.col("reading_date") == F.lit(D1)), silver)) == ["hours_reconcile", "precipitation_reconciles"]
    assert failed(gold_checks(gold.unionByName(gold), silver)) == ["unique_key", "hours_reconcile", "precipitation_reconciles"]


def test_impossible_aggregates_fail_the_checks(spark):
    gold, silver = gold_and_silver(spark)
    broken = gold.withColumn("temperature_min_c", F.lit(50.0)).withColumn("hours_observed", F.lit(25))

    assert {"hours_in_range", "temperature_order"} <= set(failed(gold_checks(broken, silver)))


def test_a_rebuild_identical_to_the_published_table_is_not_written(spark):
    gold, _ = gold_and_silver(spark)
    later = gold.withColumn("_built_at", F.current_timestamp() + F.expr("INTERVAL 1 HOUR"))

    assert not differs_from(later, gold)
    assert differs_from(later.withColumn("population", F.lit(1)), gold)
