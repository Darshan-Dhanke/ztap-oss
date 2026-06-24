.PHONY: help up down logs ps smoke edge sink-test test test-unit clean rebuild

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
	@echo "  make test      - unit tests + bring up stack + smoke + edge + sink tests"

up:
	docker compose up -d --build

down:
	docker compose down

clean:
	docker compose down -v

ps:
	docker compose ps

logs:
	docker compose logs -f

rebuild:
	docker compose up -d --build control-plane

test-unit:
	cd packages/type-engine && python -m pytest -q
	cd services/control-plane && python -m pytest -q
	cd services/sink && python -m pytest -q

smoke:
	bash scripts/smoke_test.sh

edge:
	bash scripts/edge_tests.sh

sink-test:
	bash scripts/sink_test.sh

test: test-unit up
	@echo "waiting for services to settle..."
	@sleep 20
	bash scripts/smoke_test.sh
	bash scripts/edge_tests.sh
	bash scripts/sink_test.sh
