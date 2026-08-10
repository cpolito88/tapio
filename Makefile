# tapio: developer entry point.
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

.PHONY: install
install: ## Create the venv and install all deps (including dev)
	$(UV) sync --all-extras --dev

.PHONY: lock
lock: ## Re-resolve and update uv.lock
	$(UV) lock

.PHONY: hooks
hooks: ## Install git pre-commit hooks
	$(UV) run pre-commit install

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

.PHONY: test
test: ## Run the test suite with coverage
	# `coverage run -m pytest` rather than `pytest --cov`, because tapio
	# registers a pytest plugin through an entry point. pytest imports that
	# plugin, and so most of the package, before pytest-cov starts measuring,
	# which leaves every module's import-time lines looking unexecuted and
	# reports about 54% for a suite that covers 93%.
	$(UV) run coverage run -m pytest
	$(UV) run coverage report --show-missing
	$(UV) run coverage xml

.PHONY: test-fast
test-fast: ## Run tests, stop at first failure, quiet
	$(UV) run pytest -x -q

.PHONY: cov
cov: ## Run tests and open-ready HTML coverage in htmlcov/
	$(UV) run pytest --cov=$(PKG) --cov-report=term-missing --cov-report=html

.PHONY: bench
bench: ## Run benchmarks (msg/s, spawn cost, ask latency)
	$(UV) run pytest tests/benchmarks --benchmark-only

.PHONY: bench-scale
bench-scale: ## Measure RSS and latency at 1e3/1e4/1e5 resident actors
	$(UV) run python -m tests.benchmarks.resident

.PHONY: examples
examples: ## Execute every example end to end
	$(UV) run pytest tests/examples

.PHONY: docs
docs: ## Serve the docs site with live reload
	$(UV) run mkdocs serve

.PHONY: docs-build
docs-build: ## Build the docs site into site/
	$(UV) run mkdocs build --strict

.PHONY: build
build: ## Build sdist and wheel into dist/
	$(UV) build

.PHONY: next-version
next-version: ## Print the version the commits since the last tag would produce
	$(UV) run semantic-release version --print

.PHONY: release
release: ## Tag and build from the commits (CI runs this, not you)
	# --no-commit and --no-changelog: the release is the tag, so there is
	# nothing to write into the tree and nothing to push to a protected
	# branch. The tag alone is what the build reads its version from.
	#
	# --skip-build, and the build on the next line instead: semantic-release
	# runs its own build before it creates the tag, so a build it drives
	# reads the version from a tree that is still untagged and stamps a
	# development version. Version 0.1.0 was released that way and the
	# artifacts came out as 0.1.dev22+g183cb17b6. Building after the tag
	# exists is what makes the artifact carry the release number.
	$(UV) run semantic-release version --no-commit --no-changelog --skip-build
	$(MAKE) build
	$(UV) run semantic-release publish

.PHONY: publish
publish: ## Publish to PyPI (requires UV_PUBLISH_TOKEN)
	# --check-url makes a repeat run a no-op rather than an error: an upload
	# that half succeeded, or a job somebody re-ran, skips the files PyPI
	# already has instead of failing on all of them.
	$(UV) publish --check-url https://pypi.org/simple/

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
	rm -rf dist build site htmlcov .coverage coverage.xml .pytest_cache .mypy_cache .ruff_cache
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
