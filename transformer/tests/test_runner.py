import csv
import os
from pathlib import Path

import pytest

from samples import SINGAPORE, raw_line
from transformer import open_weather
from transformer.core import runner
from transformer.normalizer.utils import parse_hour


def test_output_path_fills_template():
    template = (
        "data/{source_name}/{schema_version}"
        "/year=YYYY/month=MM/day=DD/YYYYMMDD_HHMM.csv"
    )
    got = runner.output_path_for(
        template, open_weather.TRANSFORMER, parse_hour("20261020 01")
    )
    assert got == Path(
        "data/open_weather/v1/year=2026/month=10/day=20/20261020_0100.csv"
    )


@pytest.fixture(autouse=True)
def processed_template(tmp_path, monkeypatch):
    monkeypatch.setenv(
        "PROCESSED_FILE_TEMPLATE",
        str(
            tmp_path
            / "{source_name}"
            / "{schema_version}"
            / "year=YYYY"
            / "month=MM"
            / "day=DD"
            / "YYYYMMDD_HHMM.csv"
        ),
    )


def test_run_keeps_every_row_skips_bad_rows_and_writes_pipe_csv(raw_dir):
    raw_file = raw_dir / "20261021_1400.v1.json"
    broken = {k: v for k, v in SINGAPORE.items() if k != "main"}
    raw_file.write_text(
        "\n".join(
            [
                raw_line(1, SINGAPORE),
                raw_line(2, SINGAPORE, run_at="20261021 13:59:59"),
                raw_line(3, broken),
            ]
        )
        + "\n"
    )

    result = open_weather.run(raw_file, "20261021 14", run_id="test-run")

    frontier = Path(str(raw_dir).replace("/raw/", "/v1/")) / "20261021_1400.csv"
    assert result["counts"] == {"written": 2, "error": 1}
    assert result["output_file"] == str(
        frontier.with_name("20261021_1400.v1.csv")
    )
    assert os.readlink(frontier) == "20261021_1400.v1.csv"

    with frontier.open() as f:
        rows = list(csv.DictReader(f, delimiter="|"))
    assert len(rows) == 2
    assert rows[0]["record_id"] == "1880252_1792590500"
    assert rows[0]["rain_1h_mm"] == ""
    assert (
        frontier.read_text()
        .splitlines()[0]
        .startswith("record_id|schema_version|source|")
    )


def test_run_on_frontier_records_the_versioned_raw_file(raw_dir):
    (raw_dir / "20261021_1400.v2.json").write_text(
        raw_line(1, SINGAPORE) + "\n"
    )
    (raw_dir / "20261021_1400.json").symlink_to("20261021_1400.v2.json")

    result = open_weather.run(
        raw_dir / "20261021_1400.json", "20261021 14", run_id="test-run"
    )

    assert result["input_file"] == str(raw_dir / "20261021_1400.v2.json")
    assert result["output_file"].endswith(
        "v1/year=2026/month=10/day=21/20261021_1400.v1.csv"
    )
