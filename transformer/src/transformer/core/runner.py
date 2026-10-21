import csv
import io
import json
import os
import re
from datetime import UTC, datetime
from pathlib import Path

from transformer import __version__
from transformer.core import lakefile, metrics
from transformer.core.base import Transformer
from transformer.core.logs import get_logger
from transformer.normalizer.utils import format_utc, parse_hour

_TOKENS = re.compile(
    r"\{source_name\}|\{schema_version\}|YYYYMMDD|HHMM|YYYY|MM|DD"
)


def output_path_for(
    template: str, transformer: Transformer, window_end: datetime
) -> Path:
    values = {
        "{source_name}": transformer.source,
        "{schema_version}": transformer.schema_version,
        "YYYYMMDD": f"{window_end:%Y%m%d}",
        "HHMM": f"{window_end:%H%M}",
        "YYYY": f"{window_end:%Y}",
        "MM": f"{window_end:%m}",
        "DD": f"{window_end:%d}",
    }
    return Path(_TOKENS.sub(lambda m: values[m.group()], template))


def to_csv(records: list[dict], columns: list[str]) -> str:
    buf = io.StringIO()
    writer = csv.DictWriter(
        buf, fieldnames=columns, delimiter="|", lineterminator="\n"
    )
    writer.writeheader()
    writer.writerows(records)
    return buf.getvalue()


def run(
    transformer: Transformer,
    input_path: str | Path,
    window_end: str,
    run_id: str | None = None,
) -> dict:
    started = datetime.now(UTC)
    run_id = run_id or f"manual-{started:%Y%m%dT%H%M%SZ}"
    input_path = _versioned(Path(input_path))
    log = get_logger(run_id, transformer.source)

    result = {
        "run_id": run_id,
        "status": "failure",
        "schema_version": transformer.schema_version,
        "input_file": str(input_path),
        "output_file": "",
        "file_version": 0,
        "counts": {"written": 0, "error": 0},
        "started_at": format_utc(started),
        "version": __version__,
    }
    log.info(
        "transform started",
        fields={"input_file": input_path, "version": __version__},
    )
    try:
        records, errors = _read_records(transformer, input_path, log)
        result["counts"] = {"written": len(records), "error": errors}
        output_path = output_path_for(
            os.environ["PROCESSED_FILE_TEMPLATE"],
            transformer,
            parse_hour(window_end),
        )
        output, version = lakefile.write(
            output_path, to_csv(records, transformer.columns)
        )
        result.update(
            status="success", output_file=str(output), file_version=version
        )
        log.info(
            "data saved",
            fields={"file": output, "version": version, **result["counts"]},
        )
        return result
    except Exception as exc:
        log.error("transform failed", fields={"error": repr(exc)})
        raise
    finally:
        finished = datetime.now(UTC)
        result.update(
            finished_at=format_utc(finished),
            finished_ts=finished.timestamp(),
            duration_seconds=(finished - started).total_seconds(),
        )
        _push_metrics(transformer.source, result, log)


def _versioned(path: Path) -> Path:
    # Record the file behind a frontier symlink, so lineage survives re-runs.
    return path.with_name(os.readlink(path)) if path.is_symlink() else path


def _read_records(
    transformer: Transformer, input_path: Path, log
) -> tuple[list[dict], int]:
    records, errors = [], 0
    with input_path.open() as f:
        for line in f:
            raw = json.loads(line)
            try:
                records.append(transformer.to_record(raw))
            except (KeyError, TypeError, IndexError, ValueError) as exc:
                errors += 1
                log.error(
                    "transformation error",
                    fields={"id": raw.get("id"), "error": repr(exc)},
                )
    return records, errors


def _push_metrics(source: str, result: dict, log) -> None:
    url = os.environ.get("PUSHGATEWAY_URL", "")
    if not url:
        return
    try:
        metrics.push(url, source, result)
    except Exception as exc:
        log.warning("metrics push failed", fields={"error": repr(exc)})
