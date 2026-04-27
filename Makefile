SCOPE=src/

.PHONY: format lint test clear-postgres run-pgmq-postgres


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