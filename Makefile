all: down build up test

build:
	docker compose build

up:
	docker compose up -d

down:
	docker compose down --remove-orphans

test: up
	docker compose run --rm --no-deps --entrypoint=pytest api /tests/unit /tests/integration /tests/e2e

unit-tests:
	docker compose run --rm --no-deps --entrypoint=pytest api /tests/unit

integration-tests: up
	docker compose run --rm --no-deps --entrypoint=pytest api /tests/integration

e2e-tests: up
	docker compose run --rm --no-deps --entrypoint=pytest api /tests/e2e
