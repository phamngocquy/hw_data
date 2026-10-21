# Build context is the repo root: it needs extractor/, transformer/ and orchestrator/.
FROM golang:1.25-alpine AS build
WORKDIR /src
COPY extractor/go.mod extractor/go.sum ./
RUN go mod download
COPY extractor ./
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -ldflags "-X main.version=${VERSION}" -o /out/extractor .

FROM python:3.12-slim
COPY --from=astral/uv:0.11.28 /uv /usr/local/bin/uv
ENV UV_PYTHON_DOWNLOADS=never \
    UV_COMPILE_BYTECODE=1
COPY transformer /app/transformer
COPY orchestrator /app/orchestrator
WORKDIR /app/orchestrator
RUN uv sync --frozen --no-dev
COPY --from=build /out/extractor /usr/local/bin/extractor
ENV PATH="/app/orchestrator/.venv/bin:${PATH}" \
    PREFECT_HOME=/tmp/prefect \
    LOG_FILE=/app/logs/etl.log
# Relative data paths in the .env templates resolve against /app, where ./data is mounted.
WORKDIR /app
CMD ["python", "orchestrator/flow.py"]
