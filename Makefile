# Football ELT pipeline — task runner.
#
# Every target loads .env, uses the project venv, and points dbt at
# dbt/football/ for both the project and the profile.

SHELL := /bin/bash

# Load .env into the environment for every recipe (no-op if absent).
ifneq (,$(wildcard .env))
include .env
export
endif

PY  := .venv/bin/python
DBT := .venv/bin/dbt
DBT_DIR := dbt/football
DBT_FLAGS := --project-dir $(DBT_DIR) --profiles-dir $(DBT_DIR)

.DEFAULT_GOAL := help
.PHONY: help venv-check ingest backfill daily dbt dbt-deps dbt-debug test clean

help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

venv-check: ## Fail early if the venv is missing
	@test -x $(PY) || { echo "no venv at .venv/ — see SETUP.md"; exit 1; }

ingest: venv-check ## Daily-window extract (leagues/teams/standings full + fixtures -3d/+7d)
	$(PY) -m elt.run --mode daily

backfill: venv-check ## Full extract of every endpoint for every configured league x season
	$(PY) -m elt.run --mode backfill

dbt: venv-check ## dbt build (run + test) against dbt/football
	$(DBT) build $(DBT_FLAGS)

dbt-deps: venv-check ## Install dbt packages
	$(DBT) deps $(DBT_FLAGS)

dbt-debug: venv-check ## Check dbt config + warehouse connection
	$(DBT) debug $(DBT_FLAGS)

daily: ingest dbt ## Full daily run: ingest then dbt build (dbt skipped if ingest fails)

test: venv-check ## Run the Python unit tests
	$(PY) -m pytest -q

clean: ## Remove dbt build artifacts
	rm -rf $(DBT_DIR)/target $(DBT_DIR)/dbt_packages $(DBT_DIR)/logs
