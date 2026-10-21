import json

from samples import SINGAPORE, raw_line
from transformer.open_weather import to_v1
from transformer.registry import TRANSFORMERS


def test_to_v1_flattens_and_normalizes():
    record = to_v1(json.loads(raw_line(7, SINGAPORE)))

    assert record == {
        "record_id": "1880252_1792590500",
        "schema_version": "v1",
        "source": "open_weather",
        "city_id": 1880252,
        "station_name": "Singapore",
        "country": "SG",
        "latitude": 1.29,
        "longitude": 103.8503,
        "observed_at": "20261021 13:48:20",
        "fetched_at": "20261021 13:59:30",
        "utc_offset_seconds": 28800,
        "sunrise_at": "20261020 22:46:43",
        "sunset_at": "20261021 10:51:33",
        "condition_main": "Clouds",
        "condition_description": "broken clouds",
        "temperature_c": 28.72,
        "feels_like_c": 33.78,
        "temp_min_c": 28.72,
        "temp_max_c": 28.72,
        "pressure_hpa": 1013,
        "humidity_pct": 79,
        "visibility_m": 10000,
        "wind_speed_mps": 3.76,
        "wind_direction_deg": 92,
        "wind_gust_mps": 4.32,
        "cloud_cover_pct": 72,
        "rain_1h_mm": None,
    }


def test_open_weather_is_registered_with_matching_columns():
    transformer = TRANSFORMERS["open_weather"]
    record = transformer.to_record(json.loads(raw_line(7, SINGAPORE)))

    assert transformer.schema_version == "v1"
    assert list(record) == transformer.columns
