.PHONY: run clear lint format

run:
	mkdir -p data logs
	HOST_UID=$$(id -u) HOST_GID=$$(id -g) docker compose up -d --build

clear:
	docker compose down -v
	rm -rf data logs

lint:
	cd transformer && uv run ruff check . && uv run ruff format --check .
	cd orchestrator && uv run ruff check . && uv run ruff format --check .

format:
	cd transformer && uv run ruff check --fix-only . && uv run ruff format .
	cd orchestrator && uv run ruff check --fix-only . && uv run ruff format .
