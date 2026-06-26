.PHONY: help up down logs ps smoke edge sink-test sync-test proxy-test test test-unit test-go clean rebuild

# The OLTP Postgres is Neon (docker-compose.neon.yml), so every compose command
# merges both files.
COMPOSE := docker compose -f docker-compose.yml -f docker-compose.neon.yml

help:
	@echo "ztap-oss targets:"
	@echo "  make up        - build + start the full data plane"
	@echo "  make down      - stop the stack (keeps volumes)"
	@echo "  make clean     - stop and remove volumes (DESTROYS local data)"
	@echo "  make ps        - show service status"
	@echo "  make logs      - tail all logs"
	@echo "  make test-unit - run python unit tests for both custom components"
	@echo "  make smoke     - run the end-to-end smoke test against a running stack"
	@echo "  make edge      - run the edge-case integration tests (nasty types, UC, teardown)"
	@echo "  make sink-test - run the Delta sink integration test (CDC -> Delta in MinIO)"
	@echo "  make sync-test - run the sync test (schema evolution + reverse sync)"
	@echo "  make proxy-test- run the proxy test (real container suspend/resume)"
	@echo "  make rwatch-test - run the continuous reverse-sync (inbox) test"
	@echo "  make eo-test   - run the exactly-once sink test"
	@echo "  make query-delta - register + query the lake/orders Delta table via Trino"
	@echo "  make test      - unit tests + bring up stack + all integration tests"

up:
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down

clean:
	$(COMPOSE) down -v

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f

rebuild:
	$(COMPOSE) up -d --build control-plane

test-unit:
	cd packages/type-engine && python -m pytest -q
	cd services/control-plane && python -m pytest -q
	cd services/sink && python -m pytest -q
	cd services/sync && python -m pytest -q

test-go:
	docker run --rm -v "$(CURDIR)/services/proxy":/src -w /src golang:1.22-alpine go test ./...

smoke:
	bash scripts/smoke_test.sh

edge:
	bash scripts/edge_tests.sh

sink-test:
	bash scripts/sink_test.sh

sync-test:
	bash scripts/sync_test.sh

proxy-test:
	bash scripts/proxy_test.sh

rwatch-test:
	bash scripts/reverse_watch_test.sh

eo-test:
	bash scripts/exactly_once_test.sh

query-delta:
	bash scripts/query_delta.sh

test: test-unit test-go up
	@echo "waiting for services to settle..."
	@sleep 20
	bash scripts/smoke_test.sh
	bash scripts/edge_tests.sh
	bash scripts/sink_test.sh
	bash scripts/sync_test.sh
	bash scripts/proxy_test.sh
	bash scripts/reverse_watch_test.sh
	bash scripts/exactly_once_test.sh
