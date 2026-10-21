# OpenWeather ETL

An end-to-end ETL pipeline for the Data Engineer take-home
([requirements](docs/data_side_project.pdf)). It polls the
[OpenWeather current-weather API](https://openweathermap.org/current) for **Singapore** every
30 seconds, stores every response in **Postgres**, exports it every hour into a local **data lake**
as raw JSON, and transforms it into a clean, `|`-delimited **CSV**. Logs, a health endpoint,
Prometheus metrics and a Grafana dashboard make every step observable.

- [1. High-level design](#1-high-level-design)
- [2. Repository layout](#2-repository-layout)
- [3. Running the pipeline](#3-running-the-pipeline)
- [4. Data](#4-data)
- [5. Orchestration](#5-orchestration)
- [6. Observability](#6-observability)
- [7. Configuration](#7-configuration)
- [8. Development](#8-development)
- [9. Adding a data source](#9-adding-a-data-source)
- [10. Known simplifications](#10-known-simplifications)
- [11. Productionizing](#11-productionizing)

---

## 1. High-level design

```mermaid
flowchart LR
    OW[OpenWeather API] -- every 30 s --> ING

    subgraph SERVICE[Long-running service]
        ING[ingestor<br/>Go]
    end
    ING -- INSERT raw JSON --> PG[(Postgres<br/>open_weather_raw)]

    subgraph BATCH[Prefect flow, hourly at :00 UTC]
        EXT[extractor<br/>Go CLI] -- raw file path --> TRF[transformer<br/>Python]
    end
    PG -- rows with run_at in the hour --> EXT
    EXT --> RAW[/data/open_weather/raw/…/YYYYMMDD_HHMM.json/]
    RAW --> TRF
    TRF --> V1[/data/open_weather/v1/…/YYYYMMDD_HHMM.csv/]

    ING -- scrape /metrics --> PROM[Prometheus]
    EXT -- push --> PGW[Pushgateway]
    TRF -- push --> PGW
    PGW --> PROM --> GRAF[Grafana]
```

### Components

| Module | Tech | Runs as | Responsibility |
|---|---|---|---|
| **ingestor** | Go | long-running container | Calls the API every 30 s and saves each full response in Postgres. Serves `/health` and Prometheus `/metrics`. |
| **extractor** | Go | CLI, started by Prefect | Exports the rows of one hour from Postgres to a raw JSON Lines file in the data lake. |
| **transformer** | Python | library + CLI, started by Prefect | Turns that raw file into a flat, normalized CSV. |
| **orchestrator** | Python, Prefect 3 | long-running `flow.serve()` | Runs extract → transform every hour. Hours can be re-run or backfilled from the UI. |
| **monitoring** | Prometheus, Pushgateway, Grafana | containers | Collects metrics from all three modules and shows them on a provisioned dashboard. |

Each module is its own project with its own dependencies. `ingestor/` and `extractor/` are
separate Go modules (`go.mod`); `transformer/` and `orchestrator/` are separate uv projects
(`pyproject.toml` + `uv.lock`). The orchestrator depends on the transformer package.

### How data moves

1. **Ingest (every 30 s).** The ingestor fetches the current weather and inserts the whole
   response into `open_weather_raw.data` (JSONB), with `run_at` = the time of the fetch.
2. **Extract (hourly).** For a window `[start, start + 1h)`, the extractor reads the rows whose
   `run_at` is in the window and writes them as JSON Lines to
   `data/open_weather/raw/year=YYYY/month=MM/day=DD/YYYYMMDD_HHMM.json`. The file is named after
   the window end.
3. **Transform (right after).** The transformer reads exactly that file, maps every row to the
   v1 schema (UTC timestamps, °C, cleaned text, transport fields removed), and writes
   `data/open_weather/v1/year=YYYY/month=MM/day=DD/YYYYMMDD_HHMM.csv`.
4. **Observe.** Each step logs to `logs/etl.log` and publishes metrics. The batch steps also
   publish their run's parameters and metadata.

### Design decisions

- **A service for ingestion, a batch for the rest.** Polling every 30 s is a continuous
  workload, and a long-lived process with a ticker is simpler and cheaper than starting a job
  120 times an hour. Export and transformation have no latency requirement, so they run as an
  hourly batch that is easy to re-run and backfill.
- **Postgres keeps the untransformed response.** The ingestor stores the whole response as JSONB,
  without picking or renaming fields. Postgres is the record of "what the API returned and
  when". A change in the API payload never breaks ingestion, and shaping happens downstream.
- **Two lake layers.** `raw/` holds the rows as stored; `v1/` holds the processed data, named
  after its schema version, so a future `v2/` can live next to it during a migration. Paths use
  Hive-style partitions (`year=/month=/day=`) that Spark, Athena and DuckDB understand.
- **Append-only, versioned files.** Every run, including a re-run of the same hour, writes a new
  immutable version (`….vN.json` / `….vN.csv`) and repoints a "frontier" symlink
  (`….json` / `….csv`) to it. Nothing is overwritten. Consumers read the frontier; old versions
  stay for audit and rollback.
- **Idempotent windows.** A run is fully defined by its window. The same window always reads the
  same rows and produces the same content, so re-running is safe.
- **One source name, everywhere.** Each module keeps source-specific code in its own sub-package
  (`…/openweather`, `…/open_weather`) with the source name as a constant. The same value is the
  data-lake folder, the `source` label on every metric and the `source` field on every log line.
- **Generic observability.** Metrics are shared by all sources and labelled by `source`, so a new
  source gets metrics, dashboards and alerts for free. Batch jobs exit after each run, so they push
  their metrics to a Pushgateway, including a `*_run_info` metric with the run's parameters.

---

## 2. Repository layout

```
ingestor/                    Go module: the 30 s ingestion service
  main.go                    config, starts the poller and the /health + /metrics server
  pkg/openweather/           API client, Postgres store, poller, source name
  pkg/metrics/               Prometheus collectors (labelled by source)
  pkg/logger/                log config (LOG_FILE, LOG_LEVEL)
  Dockerfile, .env.example
extractor/                   Go module: Postgres -> raw JSON Lines
  main.go                    CLI: --from "YYYYMMDD HH" [--run-id]
  pkg/openweather/           window query, raw file path, source name
  pkg/lakefile/              versioned writes + frontier symlink
  pkg/metrics/               per-run metrics pushed to the Pushgateway
  pkg/logger/                log config
  .env.example
transformer/                 uv project: raw JSON Lines -> v1 CSV
  src/transformer/
    core/                    shared by every pipeline: runner, lakefile, logs, metrics
    normalizer/utils.py      shared normalization: timestamps, units, text
    open_weather/            the OpenWeather pipeline: const.py, transform.py
    registry.py, cli.py      source -> transformer; `transformer <source> …`
  tests/, .env.example
orchestrator/                uv project: Prefect flow + "pipeline" image
  flow.py, Dockerfile
monitoring/                  Prometheus config, Grafana datasource + dashboard
docker-compose.yml, Makefile
```

---

## 3. Running the pipeline

Requires Docker with Compose v2.

```bash
cp ingestor/.env.example ingestor/.env         # set OPENWEATHER_API_KEY
cp extractor/.env.example extractor/.env
cp transformer/.env.example transformer/.env
make run
```

`make run` creates `data/` and `logs/`, then runs
`HOST_UID=$(id -u) HOST_GID=$(id -g) docker compose up -d --build`, so the containers write those
folders as your user. Always start with `make run`, not a bare `docker compose up`.

| Open | For |
|---|---|
| http://localhost:3000 | **Grafana**, dashboard *OpenWeather ETL* |
| http://localhost:4300 | **Prefect**: schedule, run history, logs, manual runs |

Only these two ports are published; set `GRAFANA_PORT` / `PREFECT_PORT` if they are taken.
Postgres, Prometheus, the Pushgateway and the ingestor stay on the internal network.

### Run a batch now

The flow runs every hour at minute 0 (UTC) and processes the previous hour. To run it now, or
for any hour:

```bash
docker compose exec pipeline prefect deployment run 'open-weather-etl/open_weather'
docker compose exec pipeline prefect deployment run 'open-weather-etl/open_weather' \
  --params '{"window": {"start": "20261020 15"}}'
```

In the Prefect UI: **Deployments → open_weather → Run → Custom run**, and fill in `start`.

### Stop and clean up

| Command | Effect |
|---|---|
| `docker compose down` | stops the stack, keeps all data |
| `make clear` | stops the stack and deletes everything it stored: the volumes, `data/` and `logs/` |

### Persistence (volumes)

| Mount | Holds |
|---|---|
| `./data` → `/app/data` (ingestor, pipeline) | the data lake: raw JSON and processed CSV |
| `./logs` → `/app/logs` (ingestor, pipeline) | `etl.log` |
| `pgdata`, `prefect`, `prometheus`, `grafana` (named volumes) | database, run history, metrics, dashboards |

The ingestor image also runs on its own with the same mounts:

```bash
docker build -t openweather-ingestor ingestor/
docker run -d --user "$(id -u):$(id -g)" --env-file ingestor/.env \
  -e DATABASE_URL="postgres://etl:etl@<host>:5432/etl?sslmode=disable" \
  -v "$(pwd)/data:/app/data" -v "$(pwd)/logs:/app/logs" \
  openweather-ingestor
```

---

## 4. Data

### Raw

Postgres table, created by the ingestor at startup:

```sql
CREATE TABLE open_weather_raw (
    id         BIGSERIAL PRIMARY KEY,
    data       JSONB       NOT NULL,   -- full API response
    run_at     TIMESTAMPTZ NOT NULL,   -- when it was fetched
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX open_weather_raw_run_at_idx ON open_weather_raw (run_at);
```

Raw file: one JSON line per row, with the row's timestamps in the shared format:

```json
{"id":2,"run_at":"20261021 16:00:01","created_at":"20261021 16:00:01","updated_at":"20261021 16:00:01","data":{…full API response…}}
```

### Data lake layout

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

- The `15:00` files hold the rows with `run_at` in `[14:00, 15:00)`.
- Both paths come from templates in the modules' `.env` files, so the layout can change without
  code changes:
  - `RAW_FILE_TEMPLATE=data/{source_name}/raw/year=YYYY/month=MM/day=DD/YYYYMMDD_HHMM.json`
  - `PROCESSED_FILE_TEMPLATE=data/{source_name}/{schema_version}/year=YYYY/month=MM/day=DD/YYYYMMDD_HHMM.csv`
- The requirements suggest `data/raw/` and `data/processed/`. Here the source comes first
  (`data/<source>/raw`, `data/<source>/v1`), so each source owns its folder and the processed
  layer carries its schema version.

### Processed (v1)

`|`-delimited CSV with a header row; an empty cell means null.

```
record_id|schema_version|source|city_id|station_name|country|latitude|longitude|observed_at|fetched_at|utc_offset_seconds|sunrise_at|sunset_at|condition_main|condition_description|temperature_c|feels_like_c|temp_min_c|temp_max_c|pressure_hpa|humidity_pct|visibility_m|wind_speed_mps|wind_direction_deg|wind_gust_mps|cloud_cover_pct|rain_1h_mm
1880252_1792598349|v1|open_weather|1880252|Singapore|SG|1.2897|103.8501|20261021 15:59:09|20261021 16:00:01|28800|20261020 22:46:43|20261021 10:51:33|Clouds|broken clouds|28.23|32.85|28.23|28.23|1013|81|10000|3.58|79|4.11|83|
```

| Group | Columns | From the API |
|---|---|---|
| Keys | `record_id` (`<city_id>_<dt>`), `schema_version`, `source` | `id`, `dt` |
| Location | `city_id`, `station_name`, `country`, `latitude`, `longitude` | `id`, `name`, `sys.country`, `coord` |
| Time (UTC) | `observed_at`, `fetched_at`, `sunrise_at`, `sunset_at`, `utc_offset_seconds` | `dt`, row `run_at`, `sys.sunrise/sunset`, `timezone` |
| Conditions | `condition_main`, `condition_description` | `weather[0]` |
| Measurements | `temperature_c`, `feels_like_c`, `temp_min_c`, `temp_max_c` (Kelvin → °C), `pressure_hpa`, `humidity_pct`, `visibility_m`, `wind_speed_mps`, `wind_direction_deg`, `wind_gust_mps`, `cloud_cover_pct`, `rain_1h_mm` | `main`, `visibility`, `wind`, `clouds`, `rain` |

Normalization:

- **Removed fields:** `base`, `cod` (transport), `weather[].icon` (UI only), `sys.type` /
  `sys.id` (internal station ids), `main.sea_level` / `main.grnd_level` (duplicate `pressure`).
- **Timestamps:** every timestamp in the data files is UTC, formatted `YYYYMMDD HH:MM:SS`
  (`20261020 16:00:00`). The local offset is kept as data in `utc_offset_seconds`.
- **Text:** control characters, symbols and the `|` delimiter are removed, and whitespace is
  collapsed (`normalizer.utils.clean_text`).
- **Every raw row becomes one CSV row.** The API refreshes about every 10 minutes, so several
  rows can share a `record_id`; it identifies the observation, so consumers can dedup on it.
  Malformed rows are logged as transformation errors and skipped.

Why this structure:

- **Flat, typed, units in the column names.** One row per observation maps 1:1 to a warehouse
  table or a Parquet file, and `temperature_c` cannot be confused with Kelvin.
- **Versioned schema.** `schema_version` and the `v1/` folder let a breaking change ship as
  `v2/` while `v1` is still produced.
- **Room for more complex data.** As sources and endpoints grow, this layer becomes a star
  schema. Location attributes move to a `dim_location` table and measurements stay in a
  `fact_weather_observation` table. One-to-many data, such as several `weather[]` conditions or
  the 5-day forecast's `list[]`, becomes child tables keyed by `record_id` instead of being
  packed into one row. Each new dataset gets its own folder under `data/<source>/`.

---

## 5. Orchestration

[`orchestrator/flow.py`](orchestrator/flow.py) defines the Prefect flow `open-weather-etl`,
served as the deployment **`open-weather-etl/open_weather`** with cron `0 * * * *` (UTC):

```
extract(start)  ── stdout: path of the raw file it wrote ──►  transform(that file)
```

- **Input:** a pydantic model, `ExtractWindow`, shown as a form in the Prefect UI with a
  description and an example.

  | Field | Format | Meaning |
  |---|---|---|
  | `start` | `YYYYMMDD HH`, e.g. `20261020 15` | The run processes `[start, start + 1h)`. Empty means the hour before the scheduled time. |

- **Run name:** `<flow>_<start>-<end>`, e.g. `open-weather-etl_20261021_16-20261021_17`.
- **extract** runs the Go binary as `extractor --from "<start>" --run-id <flow run id>`. Its
  log lines (stderr) are forwarded to the Prefect run log. A failure shows the extractor's real
  error in the UI.
- **transform** calls the transformer package in-process, with the same window and run id.
- The Prefect flow-run id is the `run_id` of both steps. It appears on every log line and on
  the run metrics, so one run can be followed across Prefect, `logs/etl.log` and Grafana.

The steps also run on their own:

```bash
docker compose exec pipeline extractor --from "20261021 16"
docker compose exec pipeline transformer open_weather \
  --input data/open_weather/raw/year=2026/month=10/day=21/20261021_1700.json \
  --window-end "20261021 17"
```

---

## 6. Observability

### Logging

All modules write key=value lines to stderr and append them to **`logs/etl.log`** (Go
`log/slog`, and a matching Python formatter). Every line has `component` and `source`; batch
lines also have `run_id`.

| Event | `msg` | Module |
|---|---|---|
| API request success / failure | `api request succeeded`, `api request failed` | ingestor |
| Data saved successfully | `data saved` (row id / file and version) | ingestor, extractor, transformer |
| Transformation errors | `transformation error` (raw row id, reason) | transformer |
| Run lifecycle | `extract started`, `extracted rows`, `transform started`, `extract failed`, `transform failed` | extractor, transformer |

```
time=2026-10-21T15:13:23.157Z level=INFO msg="api request succeeded" component=ingestor source=open_weather duration_ms=67
time=2026-10-21T15:13:39.406Z level=ERROR msg="api request failed" component=ingestor source=open_weather duration_ms=230 error="openweather status 401: …"
time=2026-10-21T16:16:09.326Z level=INFO msg="data saved" component=extractor run_id=d36dd535-… source=open_weather file=".../20261021_1620.v2.json" version=2 rows=33
time=2026-10-21T16:16:09.338Z level=INFO msg="data saved" component=transformer run_id=d36dd535-… source=open_weather file=".../20261021_1620.v2.csv" version=2 written=33 error=0
time=2026-10-21T15:13:40.579Z level=ERROR msg="transformation error" component=transformer run_id=test-run source=open_weather id=3 error=KeyError('main')
```

Each module configures its own logging from `LOG_FILE` and `LOG_LEVEL` (`debug`, `info`,
`warn`, `error`).

### Health

`GET /health` on the ingestor returns `200 ok` when Postgres answers a ping, and `503` otherwise.
It is meant for container and load-balancer health checks. Freshness is a metric rather than
part of health: an upstream outage should raise an alert, not restart a healthy process.

### Metrics

Every series carries a `source` label, so one set of metrics, panels and alerts covers every
data source.

**Ingestor** (scraped from `/metrics`):

| Metric | Why |
|---|---|
| `ingestor_api_requests_total{source,status}` | Request and error rate: the main signal for API health and rate limits. |
| `ingestor_api_request_duration_seconds{source}` | API latency (p50/p95); slow responses show up before failures. |
| `ingestor_db_inserts_total{source,status}` | Confirms fetched data was saved; separates DB problems from API problems. |
| `ingestor_last_success_timestamp_seconds{source}` | **Data freshness**: `time() - value` is the age of the newest saved row. |
| `ingestor_source_info{source,target,fetch_interval,version}` | What each source polls, how often, and which build runs. |
| `go_*`, `process_*` | Memory, goroutines, CPU and file descriptors. |

**Extractor and transformer** (pushed to the Pushgateway at the end of every run, grouped by
`{job, source}`):

| Metric | Why |
|---|---|
| `extractor_run_info{run_id, status, window_start, window_end, output_file, file_version, started_at, finished_at, version}` | **Parameters and metadata of the run**: which window, which file and version, which build. |
| `transformer_run_info{run_id, status, schema_version, input_file, output_file, file_version, started_at, finished_at, version}` | The same for the transformer, including lineage `input_file` → `output_file`. |
| `extractor_rows_extracted` | Rows in the window: about 120 per hour at a 30 s interval; fewer means ingestion gaps. |
| `transformer_records_last_run{status="written"\|"error"}` | Output size and data-quality errors. |
| `*_run_duration_seconds` | Run duration, for capacity planning. |
| `*_last_run_success` | 1/0 status of the last run. |
| `*_last_success_timestamp_seconds` | Batch freshness. Pushes replace only the metrics sent, so a failed run keeps it. |

Useful alert expressions:

```promql
time() - ingestor_last_success_timestamp_seconds > 120             # no fresh data for 4 polls
sum by (source) (rate(ingestor_api_requests_total{status="failure"}[5m]))
  / sum by (source) (rate(ingestor_api_requests_total[5m])) > 0.2   # API error rate
time() - extractor_last_success_timestamp_seconds > 2 * 3600        # hourly batch missed twice
transformer_records_last_run{status="error"} > 0                    # data-quality errors
```

### Grafana

The provisioned dashboard **OpenWeather ETL** has one row per module:

- **Ingestor:** requests per minute by status, p95 latency, inserts per minute, and time since
  the last successful fetch.
- **Extractor:** rows, duration, last status, time since the last success, and a table of the
  runs of the last 24 h with all their parameters.
- **Transformer:** records by status, duration, last status, time since the last success, and
  its run table.

A **Source** dropdown at the top filters every panel, so each pipeline can be viewed on its own
or all together.

---

## 7. Configuration

| Variable | Module | Default | Meaning |
|---|---|---|---|
| `OPENWEATHER_API_KEY` | ingestor (`ingestor/.env`) | — | API key |
| `OPENWEATHER_LOCATION`, `OPENWEATHER_LAT`, `OPENWEATHER_LON` | ingestor | `singapore`, `1.2897`, `103.8501` | location to poll |
| `FETCH_INTERVAL` | ingestor | `30s` | polling interval |
| `HTTP_ADDR` | ingestor | `:8080` | `/health` and `/metrics` address |
| `RAW_FILE_TEMPLATE` | extractor (`extractor/.env`) | — | raw file path template |
| `PROCESSED_FILE_TEMPLATE` | transformer (`transformer/.env`) | — | processed file path template |
| `DATABASE_URL` | ingestor, extractor | `postgres://etl:etl@localhost:5432/etl?sslmode=disable` | Postgres connection |
| `PUSHGATEWAY_URL` | extractor, transformer | empty (no push) | where batch metrics go |
| `LOG_FILE`, `LOG_LEVEL` | each module | `logs/etl.log`, `info` | log file and level |
| `PREFECT_API_URL` | pipeline | `http://prefect-server:4200/api` | Prefect server |
| `HOST_UID`, `HOST_GID` | compose (set by `make run`) | `1000` | user that writes `data/` and `logs/` |
| `GRAFANA_PORT`, `PREFECT_PORT` | compose | `3000`, `4300` | published UI ports |

---

## 8. Development

| Command | What |
|---|---|
| `make run` | build and start the stack |
| `make clear` | stop the stack and delete all its data |
| `make lint` | `ruff check` + `ruff format --check` for the Python projects |
| `make format` | `ruff check --fix-only` + `ruff format` (auto-fixes what `make lint` reports) |

Tests:

```bash
(cd ingestor && go test ./...)     # API client, poller metrics
(cd extractor && go test ./...)    # raw file path, versioned writes
(cd transformer && uv sync && uv run pytest)   # v1 mapping, normalizer, runner, versioned writes
```

---

## 9. Adding a data source

Each module keeps source-specific code in its own sub-package, with the source name as a
constant. A new source, say `air_quality`, needs:

| Module | Add | Reuses |
|---|---|---|
| ingestor | `pkg/airquality/` (client, store, poller), started from `main.go` | `pkg/metrics` (labelled by `source`), `pkg/logger`, `/health` + `/metrics` |
| extractor | `pkg/airquality/` (query, file path), called from `main.go` | `pkg/lakefile`, run metrics |
| transformer | `src/transformer/air_quality/` (`const.py`, `transform.py`), plus one line in `registry.py` | everything in `core/` and `normalizer/` |
| orchestrator | a flow that runs the extractor, then `air_quality.run(path, end)` | — |

On the transformer side, a pipeline only declares its columns and a row mapping. The shared
runner reads the raw file, skips and logs bad rows, writes a versioned CSV, logs and pushes
metrics. Metrics, logs, file layout and the dashboard are already keyed by `source`.

---

## 10. Known simplifications

These are intentional for a take-home and are the first things to harden:

- **No retries.** A failed API call waits for the next tick; a failed batch is re-run from Prefect.
- **No dedup.** Every poll is kept, so an observation can appear several times; `record_id` identifies it.
- **No cleanup.** Postgres rows and file versions are never pruned, and the log file is not rotated.
- **Late rows.** A row fetched in an hour that was already extracted only appears after that hour is re-run.
- **Single instance.** One ingestor, one Prefect server on SQLite, one serving process.
- **One location**, and the API key lives in `ingestor/.env` (git-ignored).

---

## 11. Productionizing

### Cloud services (AWS)

| Component | Local | Production |
|---|---|---|
| Ingestor | container | **ECS Fargate** service (or an EKS deployment), health check on `/health`, one task per group of sources |
| Raw store | Postgres container | **RDS for PostgreSQL / Aurora**: Multi-AZ, automated backups, point-in-time recovery, monthly partitions on `run_at`. For higher volume, stream raw events through **Kinesis Data Streams / Firehose** straight to S3 instead. |
| Data lake | `./data` | **S3** with the same `source/layer/year=/month=/day=` prefixes. Raw as compressed JSON; processed as **Parquet** in **Apache Iceberg** tables, whose snapshots replace the `.vN` + symlink scheme. **Glue Data Catalog** for the schema, **Athena** / Redshift Spectrum to query. |
| Batch compute | `pipeline` container | **ECS tasks** started by the orchestrator; **AWS Glue / EMR Serverless (Spark)** once one machine is not enough |
| Orchestration | Prefect server in Docker | **Prefect Cloud** with an ECS work pool, or **Amazon MWAA** (Airflow) |
| Secrets | `.env` files | **Secrets Manager** / SSM Parameter Store, injected into the tasks |
| Metrics | Prometheus + Pushgateway | **Amazon Managed Service for Prometheus** + **Amazon Managed Grafana**, or CloudWatch metrics |
| Logs | `logs/etl.log` | stdout → **CloudWatch Logs** (or Loki), structured JSON, retention policies |
| Alerting | — | Alertmanager / **CloudWatch Alarms → SNS** → Slack, PagerDuty |
| CI/CD, IaC | `make` | **GitHub Actions** (lint, tests, image build to **ECR**), **Terraform** for all infrastructure |

### Scalability

- **More sources and locations.** Each source is a sub-package behind the same
  ingest → extract → transform contract. Sources and locations are spread across ingestor
  tasks, and API calls are batched where the provider allows, within its rate limits.
- **Decouple ingestion from storage.** Publish raw responses to **Kafka / Kinesis**, so slow
  writes never block polling and several consumers (lake, database, alerting) read the same
  stream.
- **Avoid scanning the database.** Replace the hourly query with **CDC** (Debezium / AWS DMS),
  or land raw events directly in the lake.
- **Columnar processed layer.** Parquet with partition pruning, compaction of small hourly
  files into daily files, and Spark for large backfills.
- **Parallel backfills.** Windows are independent, so a backfill is many windows processed in
  parallel by Prefect.
- **Database growth.** Partition `open_weather_raw` by month and drop or archive old partitions.

### Reliability

- **Retries** with exponential backoff and jitter for API calls (honouring HTTP 429), and
  Prefect task retries for the batch steps.
- **Idempotency.** A run is keyed by `(source, window)`, and re-runs write new versions instead
  of overwriting. In Iceberg, a `MERGE` on `record_id` also removes duplicate observations.
- **Late data.** Each run also re-processes the previous one or two windows, or uses a
  watermark on `run_at`.
- **Data quality.** Schema validation (pydantic / Great Expectations), a dead-letter area for
  rejected rows, and alerts on the error count.
- **Concurrency.** A Prefect concurrency limit of 1 per source and window, and conditional
  writes (S3 `If-None-Match`) for new versions.
- **Monitoring and SLOs.** Alerts on freshness, API error rate and failed or missed runs, plus
  dashboards per source.
- **Operations.** Multi-AZ database with backups, S3 lifecycle rules (raw → Glacier), log
  retention, graceful shutdown on SIGTERM (already in place for the ingestor), and
  least-privilege IAM roles with encryption in transit and at rest.
