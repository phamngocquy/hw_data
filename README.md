# OpenWeather ETL

An ETL pipeline for the Data Engineer take-home ([requirements](docs/data_side_project.pdf)).
It polls the [OpenWeather current-weather API](https://openweathermap.org/current) for
**Singapore** every 30 seconds, stores every response in **Postgres**, exports it hourly to a
local **data lake** as raw JSON, and transforms it into a clean, `|`-delimited **CSV**.

## What's included

| Component | Tech | What it does |
|---|---|---|
| **ingestor** | Go service | Calls the API every 30 s and saves each response in Postgres. Exposes `/health` and Prometheus `/metrics`. |
| **extractor** | Go CLI | Exports one hour of rows from Postgres to `data/open_weather/raw/…/YYYYMMDD_HHMM.json` (JSON Lines). |
| **transformer** | Python (uv package) | Cleans that file into `data/open_weather/v1/…/YYYYMMDD_HHMM.csv`: flat columns, UTC timestamps, °C, cleaned text. |
| **orchestrator** | Prefect 3 | Runs extract → transform every hour at :00 UTC. Hours can be re-run from the UI. |
| **monitoring** | Prometheus, Pushgateway, Grafana | Collects metrics from all three modules and shows them on a provisioned dashboard. |

Each module is its own project with its own dependencies: `ingestor/` and `extractor/` are
separate Go modules (`go.mod`), and `transformer/` and `orchestrator/` are separate uv projects
(`pyproject.toml` + `uv.lock`).

```mermaid
flowchart LR
    OW[OpenWeather API] -- 30s --> ING[ingestor] --> PG[(Postgres)]
    PG --> EXT[extractor] --> RAW[/raw JSON/]
    RAW --> TRF[transformer] --> CSV[/v1 CSV/]
    subgraph Prefect, hourly
        EXT
        TRF
    end
    ING -- /metrics --> PROM[Prometheus]
    EXT -- push --> PGW[Pushgateway] --> PROM
    TRF -- push --> PGW
    PROM --> GRAF[Grafana]
```

## Run

Requires Docker with Compose v2.

```bash
cp ingestor/.env.example ingestor/.env         # set OPENWEATHER_API_KEY
cp extractor/.env.example extractor/.env       # RAW_FILE_TEMPLATE (output path of the extractor)
cp transformer/.env.example transformer/.env   # PROCESSED_FILE_TEMPLATE (output path of the transformer)
make run
```

`make run` creates `data/` and `logs/` and runs
`HOST_UID=$(id -u) HOST_GID=$(id -g) docker compose up -d --build`, so the containers write
those folders as your user.

| Open | For |
|---|---|
| http://localhost:3000 | **Grafana**: dashboard *OpenWeather ETL* (ingestion, extract and transform metrics) |
| http://localhost:4300 | **Prefect**: schedule, run history, manual runs |

Only these two ports are published; set `GRAFANA_PORT` / `PREFECT_PORT` on the command line if
they are taken. Postgres, Prometheus, the Pushgateway and the ingestor's `/health` and
`/metrics` stay on the internal Docker network.

The flow's input is a pydantic model (`ExtractWindow` in
[`orchestrator/flow.py`](orchestrator/flow.py)), shown as a form in the Prefect UI:

| Field | Meaning | Example |
|---|---|---|
| `start` | start hour, `YYYYMMDD HH`; the run processes `[start, start + 1h)`; empty = the hour before the scheduled time | `20261020 15` |

The extractor exports exactly the rows whose `run_at` is in `[start, start + 1h)`. Output files are
named after `start + 1h`. Scheduled runs leave `start` empty, so each run processes the previous
hour. Run it now, or for any window:

```bash
docker compose exec pipeline prefect deployment run 'open-weather-etl/open_weather'
docker compose exec pipeline prefect deployment run 'open-weather-etl/open_weather' \
  --params '{"window": {"start": "20261020 15"}}'
```

In the Prefect UI: **Deployments → open_weather → Run → Custom run**. Running the same
window again writes a new version of both files and keeps the old ones.

Stop with `docker compose down` (keeps all data). `make clear` stops the stack and deletes
everything it stored: the Postgres, Prefect, Prometheus and Grafana volumes, plus `data/` and
`logs/`.

### Persistence (volumes)

| Mount | Holds |
|---|---|
| `./data` → `/app/data` (ingestor, pipeline) | the data lake: raw JSON and processed CSV |
| `./logs` → `/app/logs` (ingestor, pipeline) | `etl.log` |
| named volumes `pgdata`, `prefect`, `prometheus`, `grafana` | database, run history, metrics, dashboards |

The ingestor image also runs on its own with the same mounts:

```bash
docker build -t openweather-ingestor ingestor/
docker run -d --user "$(id -u):$(id -g)" --env-file ingestor/.env \
  -e DATABASE_URL=postgres://etl:etl@<host>:5432/etl?sslmode=disable \
  -v "$(pwd)/data:/app/data" -v "$(pwd)/logs:/app/logs" \
  openweather-ingestor
```

## Output

```
data/open_weather/
├── raw/year=2026/month=10/day=21/
│   ├── 20261021_1500.v1.json
│   ├── 20261021_1500.v2.json                  # a re-run of the same hour
│   └── 20261021_1500.json -> 20261021_1500.v2.json
└── v1/year=2026/month=10/day=21/
    ├── 20261021_1500.v1.csv
    ├── 20261021_1500.v2.csv
    └── 20261021_1500.csv -> 20261021_1500.v2.csv
```

- One file per run, named after the window end. The `15:00` file holds the rows with `run_at` in
  `[14:00, 15:00)`.
- Data is only appended: every run or re-run writes a new `.vN` version, and old versions are
  never changed. The file without `.vN` is a symlink to the latest version; read that one.
- `v1/` is the schema version of the processed data.
- Both paths come from templates in the modules' `.env` files:
  - `RAW_FILE_TEMPLATE=data/{source_name}/raw/year=YYYY/month=MM/day=DD/YYYYMMDD_HHMM.json`
  - `PROCESSED_FILE_TEMPLATE=data/{source_name}/{schema_version}/year=YYYY/month=MM/day=DD/YYYYMMDD_HHMM.csv`

All timestamps in the data files are UTC, formatted `YYYYMMDD HH:MM:SS` (e.g.
`20261020 16:00:00`). The extractor takes whole hours, `YYYYMMDD HH`:
`extractor --from "20261020 16"` (exports `[16:00, 17:00)`).

Processed CSV (`|` delimiter, header row, empty cell = null):

```
record_id|schema_version|source|city_id|station_name|country|latitude|longitude|observed_at|fetched_at|utc_offset_seconds|sunrise_at|sunset_at|condition_main|condition_description|temperature_c|feels_like_c|temp_min_c|temp_max_c|pressure_hpa|humidity_pct|visibility_m|wind_speed_mps|wind_direction_deg|wind_gust_mps|cloud_cover_pct|rain_1h_mm
1880252_1792598349|v1|open_weather|1880252|Singapore|SG|1.2897|103.8501|20261021 15:59:09|20261021 16:00:01|28800|20261020 22:46:43|20261021 10:51:33|Clouds|broken clouds|28.23|32.85|28.23|28.23|1013|81|10000|3.58|79|4.11|83|
```

The transformer drops transport and UI fields (`base`, `cod`, `weather[].icon`, `sys.type/id`,
`sea_level`, `grnd_level`), and writes one flat row per raw row (`record_id` =
`<city_id>_<observed_at>`). The API refreshes about every 10 minutes, so consecutive polls often
repeat the same observation.

## Logs and metrics

**Logs:** all modules append key=value lines to `logs/etl.log`. These cover API success and
failure, saved data, transformation errors and the start and end of each run. Every line has
`component` and `source`; batch lines also have the Prefect `run_id`. Each module sets its
own `LOG_FILE` and `LOG_LEVEL`.

```
time=2026-10-21T15:13:23.157Z level=INFO msg="api request succeeded" component=ingestor source=open_weather duration_ms=67
time=2026-10-21T16:16:09.326Z level=INFO msg="data saved" component=extractor run_id=d36dd535-… source=open_weather file=".../20261021_1620.v2.json" version=2 rows=33
time=2026-10-21T16:16:09.338Z level=INFO msg="data saved" component=transformer run_id=d36dd535-… source=open_weather file=".../20261021_1620.v2.csv" version=2 written=33 error=0
```

**Metrics:** every series has a `source` label, so each data source is shown separately.

| Module | Metrics |
|---|---|
| ingestor (scraped) | `ingestor_api_requests_total{status}`, `ingestor_api_request_duration_seconds`, `ingestor_db_inserts_total{status}`, `ingestor_last_success_timestamp_seconds`, `ingestor_source_info` |
| extractor (pushed) | `extractor_run_info{run_id, window_start, window_end, output_file, file_version, …}`, `extractor_rows_extracted`, `extractor_run_duration_seconds`, `extractor_last_run_success`, `extractor_last_success_timestamp_seconds` |
| transformer (pushed) | `transformer_run_info{run_id, input_file, output_file, file_version, …}`, `transformer_records_last_run{status=written\|error}`, `transformer_run_duration_seconds`, `transformer_last_run_success`, `transformer_last_success_timestamp_seconds` |

The `*_run_info` metrics carry each batch run's parameters and metadata. The Grafana dashboard
shows them as a table of recent runs. A **Source** dropdown at the top filters every panel by
`source`, so each pipeline can be viewed on its own or all together.

## Tests and lint

```bash
(cd ingestor && go test ./...)
(cd extractor && go test ./...)
cd transformer && uv sync && uv run pytest
```

```bash
make lint      # ruff check + ruff format --check (transformer, orchestrator)
make format    # ruff check --fix + ruff format
```

Each Python project has its own ruff config (`[tool.ruff]` in its `pyproject.toml`).

## Adding a data source

Each module keeps source-specific code in its own sub-package, with the source name as a
constant:
- **ingestor:** `ingestor/pkg/<source>/` (client, store, poller), started from `main.go`;
- **extractor:** `extractor/pkg/<source>/`;
- **transformer:** `transformer/src/transformer/<source>/` (`const.py`, `transform.py`), plus one
  line in `registry.py`.

Metrics, logs, file layout and dashboards are shared and keyed by `source`. Common
normalization (UTC timestamps, unit conversion, `clean_text` for special characters) lives in
`transformer/src/transformer/normalizer/utils.py`.

## Known simplifications

- No retries. A failed API call waits for the next tick; a failed batch is re-run from Prefect.
- No dedup: every poll is kept, so the same observation can appear many times. Use `record_id`
  to dedup downstream.
- Old rows and file versions are never cleaned up, and the log file is not rotated.
- One location; the API key is kept in `ingestor/.env` (git-ignored).

## Productionizing

| Part | AWS option |
|---|---|
| Ingestor | ECS Fargate or EKS, health check on `/health` |
| Database | RDS / Aurora PostgreSQL, or skip it and stream raw events through Kinesis Firehose to S3 |
| Data lake | S3 with the same `source/layer/year=/month=/day=` layout, Parquet in Apache Iceberg tables, Glue Data Catalog, queried with Athena |
| Orchestration | Prefect Cloud with an ECS work pool, or MWAA (Airflow) |
| Secrets | Secrets Manager |
| Monitoring | Amazon Managed Prometheus + Managed Grafana, CloudWatch Logs, alerts to Slack or PagerDuty |
| CI/CD | GitHub Actions (tests, image build to ECR), Terraform |

**Scalability:** add sources as new sub-packages and ingestor workers. Put Kafka or Kinesis
between polling and storage. Replace the hourly DB scan with CDC (Debezium / DMS). Move the
processed layer to Parquet, compact the hourly files, and use Spark when one machine is no
longer enough. Hours are independent, so backfills run in parallel.

**Reliability:** retries with backoff for API calls and batch tasks. Idempotent writes keyed by
`record_id` (Iceberg `MERGE`). A dead-letter area for bad rows, and data-quality checks.
Alerts on data freshness, error rate and failed runs. Retention policies for the DB, files and
logs. Least-privilege IAM and encryption in transit and at rest.
