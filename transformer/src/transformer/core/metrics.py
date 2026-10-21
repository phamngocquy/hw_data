from prometheus_client import CollectorRegistry, Gauge, pushadd_to_gateway

INFO_LABELS = [
    "run_id",
    "status",
    "schema_version",
    "input_file",
    "output_file",
    "file_version",
    "started_at",
    "finished_at",
    "version",
]


def push(url: str, source: str, run: dict) -> None:
    # pushadd (POST) replaces only the metrics sent, so a failed run keeps
    # the last success timestamp.
    registry = CollectorRegistry()

    info = Gauge(
        "transformer_run_info",
        "Parameters and metadata of the last transformer run (always 1).",
        INFO_LABELS,
        registry=registry,
    )
    info.labels(**{label: str(run[label]) for label in INFO_LABELS}).set(1)

    records = Gauge(
        "transformer_records_last_run",
        "Records in the last run by status.",
        ["status"],
        registry=registry,
    )
    for status, count in run["counts"].items():
        records.labels(status).set(count)

    succeeded = run["status"] == "success"
    _gauge(
        registry,
        "transformer_run_duration_seconds",
        "Duration of the last run.",
        run["duration_seconds"],
    )
    _gauge(
        registry,
        "transformer_last_run_success",
        "1 if the last run succeeded, else 0.",
        int(succeeded),
    )
    if succeeded:
        _gauge(
            registry,
            "transformer_last_success_timestamp_seconds",
            "Unix time of the last successful run.",
            run["finished_ts"],
        )

    pushadd_to_gateway(
        url,
        job="transformer",
        grouping_key={"source": source},
        registry=registry,
    )


def _gauge(
    registry: CollectorRegistry, name: str, help_text: str, value: float
) -> None:
    Gauge(name, help_text, registry=registry).set(value)
