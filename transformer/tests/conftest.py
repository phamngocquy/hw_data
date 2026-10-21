import os
import tempfile

import pytest

# Keep test runs out of the real logs/etl.log and never push metrics.
os.environ["LOG_FILE"] = os.path.join(tempfile.mkdtemp(), "etl.log")
os.environ.pop("PUSHGATEWAY_URL", None)


@pytest.fixture
def raw_dir(tmp_path):
    path = (
        tmp_path / "open_weather" / "raw" / "year=2026" / "month=10" / "day=21"
    )
    path.mkdir(parents=True)
    return path
