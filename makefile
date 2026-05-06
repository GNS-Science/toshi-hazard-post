sources = toshi_hazard_post

.PHONY: test format lint unittest coverage pre-commit clean
test: format lint unittest

format:
	uv run ruff format $(sources) tests

lint:
	uv run ruff check $(sources) tests
	uv run mypy $(sources) tests

unittest:
	uv run pytest

coverage:
	uv run pytest --cov=$(sources) --cov-branch --cov-report=term-missing tests

pre-commit:
	uv run pre-commit run --all-files

clean:
	rm -rf .mypy_cache .pytest_cache
	rm -rf *.egg-info
	rm -rf .tox dist site
	rm -rf coverage.xml .coverage
