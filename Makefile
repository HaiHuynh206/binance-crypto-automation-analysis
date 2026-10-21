SHELL := /bin/bash
PYTHON ?= python3
VENV := .venv
BIN := $(VENV)/bin

.PHONY: help venv install test lint ingest backfill status catalog sync-s3 up down logs clean permissions share unshare migrate

help:
	@echo "make install    - create .venv and install the package (editable)"
	@echo "make ingest     - ingest the last 2 hours"
	@echo "make backfill   - backfill DAYS=7"
	@echo "make status     - show lake size and coverage"
	@echo "make catalog    - rebuild DuckDB views"
	@echo "make sync-s3    - upload the lake to S3/R2/MinIO (DRY_RUN=1 to preview)"
	@echo "make test       - run unit tests"
	@echo "make up/down    - start/stop the Airflow stack (Docker)"
	@echo "make permissions- share data/ between host CLI and Airflow container"
	@echo "make share      - expose the Airflow UI via a Cloudflare quick tunnel"
	@echo "make migrate    - migrate to a VPS: make migrate HOST=root@1.2.3.4"

venv:
	test -d $(VENV) || $(PYTHON) -m venv $(VENV)

install: venv
	$(BIN)/pip install -U pip
	$(BIN)/pip install -e ".[dev]"

test: install
	$(BIN)/pytest -q

lint: install
	$(BIN)/ruff check src tests dags

ingest: install
	@set -a; [ -f .env ] && . ./.env; set +a; $(BIN)/crypto-ingest ingest --hours 2

backfill: install
	@set -a; [ -f .env ] && . ./.env; set +a; $(BIN)/crypto-ingest backfill --days $(or $(DAYS),7)

status: install
	@set -a; [ -f .env ] && . ./.env; set +a; $(BIN)/crypto-ingest status

catalog: install
	@set -a; [ -f .env ] && . ./.env; set +a; $(BIN)/crypto-ingest catalog

sync-s3: install
	@set -a; [ -f .env ] && . ./.env; set +a; $(BIN)/crypto-ingest sync-s3 $(if $(DRY_RUN),--dry-run,) $(if $(FORCE),--force,)

.env:
	cp .env.example .env

up: .env
	mkdir -p data
	chmod 777 data
	docker compose up -d --build

permissions:
	mkdir -p data
	sudo setfacl -R -m u:$(shell id -u):rwx -m u:50000:rwx data
	sudo setfacl -R -d -m u:$(shell id -u):rwx -m u:50000:rwx data

share:
	./scripts/share-ui.sh

unshare:
	./scripts/stop-share.sh

migrate:
	@test -n "$(HOST)" || (echo "Usage: make migrate HOST=root@1.2.3.4 [WITH_DATA=1]" && exit 1)
	./scripts/migrate-to-vps.sh "$(HOST)" $(if $(WITH_DATA),--with-data,)

down:
	docker compose down

logs:
	docker compose logs -f airflow-scheduler

clean:
	rm -rf $(VENV) data .pytest_cache
