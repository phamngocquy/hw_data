import subprocess
from datetime import UTC, datetime, timedelta

from prefect import flow, get_run_logger, task
from prefect.runtime import flow_run
from pydantic import BaseModel, ConfigDict, Field
from transformer import open_weather
from transformer.normalizer.utils import HOUR_FORMAT


class ExtractWindow(BaseModel):
    """Which raw rows to process: those with run_at in [start, start + 1h)."""

    model_config = ConfigDict(
        frozen=True, json_schema_extra={"examples": [{"start": "20261020 15"}]}
    )

    start: str | None = Field(
        default=None,
        pattern=r"^\d{8} \d{2}$",
        description=(
            'Start hour, "YYYYMMDD HH". The run processes [start, start + 1h); '
            "output files are named after start + 1h (…/20261020_1600.json). "
            "Leave empty for the hour before the scheduled time."
        ),
        examples=["20261020 15"],
    )


PREVIOUS_HOUR = ExtractWindow()


def window_hours(start: str | None) -> tuple[datetime, datetime]:
    if start:
        begin = datetime.strptime(start, HOUR_FORMAT)
    else:
        scheduled = flow_run.scheduled_start_time.astimezone(UTC)
        begin = scheduled.replace(
            minute=0, second=0, microsecond=0
        ) - timedelta(hours=1)
    return begin, begin + timedelta(hours=1)


def run_name() -> str:
    window = flow_run.parameters.get("window") or {}
    start = window.get("start") if isinstance(window, dict) else window.start
    begin, end = window_hours(start)
    return f"{flow_run.flow_name}_{begin:%Y%m%d_%H}-{end:%Y%m%d_%H}"


@task
def extract(start: str) -> str:
    """Returns the raw file the extractor prints; its stderr goes to Prefect."""
    result = subprocess.run(
        ["extractor", "--from", start, "--run-id", flow_run.id],
        capture_output=True,
        text=True,
    )
    log = get_run_logger()
    stderr = result.stderr.strip().splitlines()
    for line in stderr:
        (log.error if "level=ERROR" in line else log.info)(line)
    if result.returncode != 0:
        reason = stderr[-1] if stderr else "no output"
        raise RuntimeError(
            f"extractor exited with code {result.returncode}: {reason}"
        )
    return result.stdout.strip().splitlines()[-1]


@task
def transform(input_path: str, end: str) -> dict:
    return open_weather.run(input_path, end, run_id=flow_run.id)


@flow(name="open-weather-etl", flow_run_name=run_name)
def open_weather_etl(window: ExtractWindow = PREVIOUS_HOUR) -> dict:
    """Extract rows with run_at in [start, start + 1h) and make the v1 CSV."""
    begin, end = window_hours(window.start)
    return transform(
        extract(begin.strftime(HOUR_FORMAT)), end.strftime(HOUR_FORMAT)
    )


if __name__ == "__main__":
    open_weather_etl.serve(name="open_weather", cron="0 * * * *")
