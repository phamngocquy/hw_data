import json
import logging
import os
from datetime import UTC, datetime
from pathlib import Path

_logger = logging.getLogger("transformer")


class _KeyValueFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        time = datetime.fromtimestamp(record.created, UTC).isoformat(
            timespec="milliseconds"
        )
        fields = {
            "time": time.replace("+00:00", "Z"),
            "level": record.levelname,
            "msg": record.getMessage(),
            **getattr(record, "fields", {}),
        }
        return " ".join(
            f"{key}={_quote(value)}" for key, value in fields.items()
        )


def _quote(value) -> str:
    text = str(value)
    return json.dumps(text) if any(c in text for c in ' "=') else text


class _RunLogger(logging.LoggerAdapter):
    """Extra fields per call go in `fields={...}`."""

    def process(self, msg, kwargs):
        fields = {**self.extra, **kwargs.pop("fields", {})}
        return msg, {**kwargs, "extra": {"fields": fields}}


def setup(log_file: str, level: str) -> None:
    numeric_level = logging.getLevelNamesMapping().get(level.upper())
    if numeric_level is None:
        raise ValueError(f"LOG_LEVEL: unknown level {level!r}")

    Path(log_file).parent.mkdir(parents=True, exist_ok=True)
    for handler in list(_logger.handlers):
        _logger.removeHandler(handler)
        handler.close()
    formatter = _KeyValueFormatter()
    for handler in (logging.StreamHandler(), logging.FileHandler(log_file)):
        handler.setFormatter(formatter)
        _logger.addHandler(handler)
    _logger.setLevel(numeric_level)
    _logger.propagate = False


def get_logger(run_id: str, source: str) -> logging.LoggerAdapter:
    if not _logger.handlers:
        setup(
            os.environ.get("LOG_FILE", "logs/etl.log"),
            os.environ.get("LOG_LEVEL", "info"),
        )
    return _RunLogger(
        _logger,
        {"component": "transformer", "run_id": run_id, "source": source},
    )
