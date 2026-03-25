.PHONY: init test lint validate-data validate-challenge validate-challenge-strict run-bronze run-silver run-q1 run-all clean dashboard airflow docker-build docker-up docker-down

# Tras ./scripts/setup.sh o make init: preferir el intérprete del venv
PY_SYS := $(shell command -v python3)
PYTHON := $(shell if [ -x venv/bin/python ]; then echo venv/bin/python; else echo $(PY_SYS); fi)

export PYTHONPATH := $(shell pwd)

# Valida Python vs pyproject.toml, opcionalmente instala python3-venv (y JDK) en Ubuntu/Debian
# Ejemplo: make init ARGS='--install-system-deps --with-java'
ARGS ?=

init:
	@chmod +x scripts/setup.sh 2>/dev/null || true
	./scripts/setup.sh $(ARGS)

test:
	$(PYTHON) -m pytest tests/ -v --cov=src --cov-report=term-missing

# EDA: Data Dictionary.xlsx vs data/raw (pandas/pyarrow; no Spark)
validate-data:
	$(PYTHON) scripts/validate_data_dictionary.py

# PDF del challenge vs salidas Gold (pandas; lee todos los part-*.parquet)
# Opciones extra al script: make validate-challenge CHALLENGE_VALIDATE_ARGS='--json /tmp/r.json'
# No uses "make validate-challenge --strict" (make interpreta --strict como su propia opción).
CHALLENGE_VALIDATE_ARGS ?=
validate-challenge:
	$(PYTHON) scripts/validate_challenge_results.py $(CHALLENGE_VALIDATE_ARGS)

# Exit 1 si algún hallazgo tiene severity=error (útil para CI)
validate-challenge-strict:
	$(PYTHON) scripts/validate_challenge_results.py --strict

lint:
	$(PYTHON) -m black src/ pipelines/ dashboard/ tests/
	$(PYTHON) -m isort src/ pipelines/ dashboard/ tests/
	$(PYTHON) -m flake8 src/ pipelines/ dashboard/ tests/
	$(PYTHON) -m mypy src/

run-bronze:
	$(PYTHON) -m pipelines.bronze_ingestion --env dev

run-silver:
	$(PYTHON) -m pipelines.silver_builder --env dev

run-q1:
	$(PYTHON) -m pipelines.runner --question q1 --env dev

run-all:
	$(PYTHON) -m pipelines.bronze_ingestion --env dev && \
	$(PYTHON) -m pipelines.silver_builder --env dev && \
	$(PYTHON) -m pipelines.runner --all --env dev

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	rm -rf data/bronze data/silver data/gold 2>/dev/null || true

dashboard:
	$(PYTHON) -m streamlit run dashboard/app.py

airflow:
	@echo "Opción A (Docker): make docker-up  → UI en :8080"
	@echo "Opción B (local): source venv/bin/activate && airflow standalone  (o webserver + scheduler)"

docker-build:
	docker compose build

docker-up:
	AIRFLOW_UID=$(shell id -u) docker compose up -d --build

docker-down:
	docker compose down
