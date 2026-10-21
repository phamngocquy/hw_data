from pathlib import Path

from transformer.core import runner
from transformer.core.base import Transformer
from transformer.normalizer.utils import (
    clean_text,
    epoch_to_utc,
    kelvin_to_celsius,
)
from transformer.open_weather.const import SCHEMA_VERSION, SOURCE

COLUMNS = [
    "record_id",
    "schema_version",
    "source",
    "city_id",
    "station_name",
    "country",
    "latitude",
    "longitude",
    "observed_at",
    "fetched_at",
    "utc_offset_seconds",
    "sunrise_at",
    "sunset_at",
    "condition_main",
    "condition_description",
    "temperature_c",
    "feels_like_c",
    "temp_min_c",
    "temp_max_c",
    "pressure_hpa",
    "humidity_pct",
    "visibility_m",
    "wind_speed_mps",
    "wind_direction_deg",
    "wind_gust_mps",
    "cloud_cover_pct",
    "rain_1h_mm",
]


def to_v1(raw: dict) -> dict:
    data = raw["data"]
    main, sys, weather = data["main"], data["sys"], data["weather"][0]
    wind = data.get("wind", {})
    return {
        "record_id": f"{data['id']}_{data['dt']}",
        "schema_version": SCHEMA_VERSION,
        "source": SOURCE,
        "city_id": data["id"],
        "station_name": clean_text(data["name"]),
        "country": clean_text(sys["country"]),
        "latitude": data["coord"]["lat"],
        "longitude": data["coord"]["lon"],
        "observed_at": epoch_to_utc(data["dt"]),
        "fetched_at": raw["run_at"],
        "utc_offset_seconds": data["timezone"],
        "sunrise_at": epoch_to_utc(sys["sunrise"]),
        "sunset_at": epoch_to_utc(sys["sunset"]),
        "condition_main": clean_text(weather["main"]),
        "condition_description": clean_text(weather["description"]),
        "temperature_c": kelvin_to_celsius(main["temp"]),
        "feels_like_c": kelvin_to_celsius(main["feels_like"]),
        "temp_min_c": kelvin_to_celsius(main["temp_min"]),
        "temp_max_c": kelvin_to_celsius(main["temp_max"]),
        "pressure_hpa": main["pressure"],
        "humidity_pct": main["humidity"],
        "visibility_m": data.get("visibility"),
        "wind_speed_mps": wind.get("speed"),
        "wind_direction_deg": wind.get("deg"),
        "wind_gust_mps": wind.get("gust"),
        "cloud_cover_pct": data.get("clouds", {}).get("all"),
        "rain_1h_mm": data.get("rain", {}).get("1h"),
    }


TRANSFORMER = Transformer(
    source=SOURCE,
    schema_version=SCHEMA_VERSION,
    columns=COLUMNS,
    to_record=to_v1,
)


def run(
    input_path: str | Path, window_end: str, run_id: str | None = None
) -> dict:
    return runner.run(TRANSFORMER, input_path, window_end, run_id)
