PART ?= patch
VERSION ?=
DRY_RUN ?=

RELEASE_ARGS = --part $(PART)
ifneq ($(strip $(VERSION)),)
RELEASE_ARGS = --version $(VERSION)
endif
ifneq ($(strip $(DRY_RUN)),)
RELEASE_ARGS += --dry-run
endif

.PHONY: help sync test lint format check release release-minor release-major release-dry

help:
	@echo "Available targets:"
	@echo "  make sync           - install dependencies with uv"
	@echo "  make test           - run pytest"
	@echo "  make check          - ruff format check + lint + tests"
	@echo "  make release        - patch bump, tag, roll v1, push"
	@echo "  make release-minor  - minor bump"
	@echo "  make release-major  - major bump (rolls v2, consumers stay on v1)"
	@echo "  make release-dry    - show what a patch release would do"
	@echo ""
	@echo "  make release VERSION=1.3.0   - release an explicit version"

sync:
	uv sync --group dev

test:
	uv run pytest -q

lint:
	uv run ruff check .

format:
	uv run ruff format .

check:
	uv run ruff format --check .
	uv run ruff check .
	uv run pytest -q

release: check
	uv run python scripts/release.py $(RELEASE_ARGS)

release-minor:
	$(MAKE) release PART=minor

release-major:
	$(MAKE) release PART=major

release-dry:
	uv run python scripts/release.py --part $(PART) --dry-run
