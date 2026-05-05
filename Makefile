SCOPE=src/

.PHONY: format lint test clear-postgres run-pgmq-postgres docs-serve docs-build docs-deploy


docs-serve:
	uv run --group docs mkdocs serve

docs-build:
	uv run --group docs mkdocs build

docs-deploy:
	@if [ -z "$(VERSION)" ]; then echo "VERSION is required, e.g. make docs-deploy VERSION=1.1.0"; exit 1; fi
	uv run --group docs mike deploy $(VERSION) latest
	uv run --group docs mike set-default latest

format:
	uv run ruff format $(SCOPE)
	uv run ruff check --fix --exit-zero $(SCOPE)

lint:
	uv run ruff check $(SCOPE)
	uv run ruff format --check $(SCOPE)

clear-postgres:
	docker rm -f pgmq-postgres || true

run-pgmq-postgres:
	docker run -d --name pgmq-postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 ghcr.io/pgmq/pg18-pgmq:latest

test: clear-postgres run-pgmq-postgres
	sleep 10  # Give PostgreSQL time to start
	uv run python -m unittest discover -s tests -p "test_*.py"

test-env:
	uv run python -m unittest discover -s tests -p "test_*.py"