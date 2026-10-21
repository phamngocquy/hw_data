from transformer.normalizer.utils import (
    clean_text,
    epoch_to_utc,
    kelvin_to_celsius,
    parse_hour,
)


def test_time_formats():
    assert epoch_to_utc(1792590500) == "20261021 13:48:20"
    assert parse_hour("20261021 13").isoformat() == "2026-10-21T13:00:00+00:00"


def test_kelvin_to_celsius():
    assert kelvin_to_celsius(301.87) == 28.72


def test_clean_text_removes_special_characters():
    assert clean_text("  broken\tclouds |☁\x00 ") == "broken clouds"
    assert clean_text("Xóm Phố") == "Xóm Phố"
    assert clean_text("|||") is None
    assert clean_text(None) is None
