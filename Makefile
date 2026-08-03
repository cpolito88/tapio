# tapio — developer entry point.
#
# This file is the single source of truth for what CI runs: the pipeline calls
# `make ci` rather than re-spelling these commands, so local and CI cannot
# silently diverge. Tool configuration lives in pyproject.toml; targets here
# only sequence it.

.DEFAULT_GOAL := help
SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c

UV   := uv
SRC  := src/tapio
PKG  := tapio
EXPL := examples/tapio_examples

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

# --- setup -------------------------------------------------------------------

.PHONY: install
install: ## Create the venv and install all deps (including dev)
	$(UV) sync --all-extras --dev

.PHONY: lock
lock: ## Re-resolve and update uv.lock
	$(UV) lock

.PHONY: hooks
hooks: ## Install git pre-commit hooks
	$(UV) run pre-commit install

# --- quality -----------------------------------------------------------------

.PHONY: fmt
fmt: ## Auto-format and apply safe lint fixes
	$(UV) run ruff format .
	$(UV) run ruff check --fix .

.PHONY: lint
lint: ## Check formatting and lint rules (no writes)
	$(UV) run ruff format --check .
	$(UV) run ruff check .

.PHONY: type
type: ## Type-check under mypy strict
	$(UV) run mypy --strict $(SRC) $(EXPL)

.PHONY: check
check: lint type test ## Pre-push gate: lint + types + tests

# --- testing -----------------------------------------------------------------

.PHONY: test
test: ## Run the test suite
	$(UV) run pytest

.PHONY: test-fast
test-fast: ## Run tests, stop at first failure, quiet
	$(UV) run pytest -x -q

.PHONY: cov
cov: ## Run tests with a coverage report
	$(UV) run pytest --cov=$(PKG) --cov-report=term-missing --cov-report=html

.PHONY: bench
bench: ## Run benchmarks (msg/s, spawn cost, ask latency)
	$(UV) run pytest tests/benchmarks --benchmark-only

.PHONY: examples
examples: ## Execute every example end to end
	$(UV) run pytest tests/examples

# --- docs --------------------------------------------------------------------

.PHONY: docs
docs: ## Serve the docs site with live reload
	$(UV) run mkdocs serve

.PHONY: docs-build
docs-build: ## Build the docs site into site/
	$(UV) run mkdocs build --strict

# --- release -----------------------------------------------------------------

.PHONY: build
build: ## Build sdist and wheel into dist/
	$(UV) build

.PHONY: publish
publish: ## Publish to PyPI (requires UV_PUBLISH_TOKEN)
	$(UV) publish

# --- meta --------------------------------------------------------------------

.PHONY: ci
ci: ## What GitHub Actions runs: locked install, then the full gate
	$(UV) sync --all-extras --dev --frozen
	$(MAKE) lint
	$(MAKE) type
	$(MAKE) test
	$(MAKE) examples
	$(MAKE) docs-build

.PHONY: clean
clean: ## Remove build, cache, and coverage artifacts
	rm -rf dist build site htmlcov .coverage .pytest_cache .mypy_cache .ruff_cache
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
