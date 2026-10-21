import re
from datetime import UTC, datetime

# Shared by all data files, always UTC: 20261020 16:00:00.
TIME_FORMAT = "%Y%m%d %H:%M:%S"
# Input windows are whole hours: 20261020 16.
HOUR_FORMAT = "%Y%m%d %H"

# Keep letters/digits of any language, whitespace and basic punctuation;
# drop the rest, including the CSV delimiter "|".
_SPECIAL_CHARS = re.compile(r"[^\w\s\-'.,()/&]")


def format_utc(dt: datetime) -> str:
    return dt.astimezone(UTC).strftime(TIME_FORMAT)


def epoch_to_utc(epoch: int) -> str:
    return format_utc(datetime.fromtimestamp(epoch, UTC))


def parse_hour(value: str) -> datetime:
    return datetime.strptime(value, HOUR_FORMAT).replace(tzinfo=UTC)


def kelvin_to_celsius(kelvin: float) -> float:
    return round(kelvin - 273.15, 2)


def clean_text(value: str | None) -> str | None:
    """Empty text becomes None, i.e. a null CSV cell."""
    if value is None:
        return None
    text = " ".join(_SPECIAL_CHARS.sub("", value).split())
    return text or None
