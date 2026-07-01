PYTHON ?= python
COMPOSE ?= docker compose

.PHONY: install-dev test lint format typecheck config-print compose-up compose-full-up compose-down

install-dev:
	$(PYTHON) -m pip install -e .[api,dev]

test:
	$(PYTHON) -m pytest tests/ -v

lint:
	$(PYTHON) -m ruff check .

format:
	$(PYTHON) -m ruff format .

typecheck:
	$(PYTHON) -m mypy src tests

config-print:
	$(PYTHON) scripts/print_settings.py

compose-up:
	$(COMPOSE) up -d

compose-full-up:
	$(COMPOSE) --profile compute --profile orchestration --profile bi up -d

compose-down:
	$(COMPOSE) down
