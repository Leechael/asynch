#!make

# Load local .env file
-include .env
export

DIRS = asynch/ tests/ benchmark/
PY_DEBUG_OPTS = PYTHONDEVMODE=1 PYTHONTRACEMALLOC=1

up:
	pdm update

deps:
	pdm install -G lint -G test -G compression --without dev --no-self

bench: deps
	pdm install -G dev -G compression --no-self
	pdm run python3 benchmark/main.py

check:
	pdm run ruff format --check $(DIRS)
	pdm run ruff check $(DIRS)

lint:
	pdm run ruff format $(DIRS)
	pdm run ruff check --fix $(DIRS)

test:
	$(PY_DEBUG_OPTS) pdm run pytest

build: deps clean
	pdm build

clean:
	rm -rf ./dist

ci: check test
