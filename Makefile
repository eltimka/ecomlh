# ============================================================================
# E-Commerce Customer 360 Lakehouse - one-command entry points
#
# Fastest path (fresh machine):
#   python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
#   cp .env.example .env
#   make run                      # everything: infra + data + pipeline + dashboard
#
# Individual steps (all idempotent):
#   make up / down / bootstrap / refresh / dev / verify / verify-dq
# ============================================================================

VENV := $(CURDIR)/.venv/bin

.PHONY: run up down bootstrap refresh dev dashboard verify verify-dq logs clean

## run: one-command end-to-end startup (compose up -> data -> bootstrap ->
##      full refresh with all checks -> quick verification suite)
run:
	$(VENV)/python scripts/start_all.py

## up: start the docker stack and wait until healthy
up:
	docker compose -f docker/docker-compose.yml up -d
	docker compose -f docker/docker-compose.yml ps

## down: stop the docker stack and the dashboard (data volumes are kept)
down:
	docker compose -f docker/docker-compose.yml down
	@if [ -f .logs/dashboard.pid ]; then kill `cat .logs/dashboard.pid` 2>/dev/null; rm -f .logs/dashboard.pid; echo "dashboard stopped"; fi

## bootstrap: MinIO buckets + Trino schemas (idempotent)
bootstrap:
	$(VENV)/python scripts/bootstrap_minio.py

## refresh: materialize bronze -> silver -> gold with all 85 DQ checks
refresh:
	cd dagster_project && $(VENV)/dagster job execute -m ecommerce_lakehouse.definitions -j lakehouse_refresh

## dev: Dagster UI on http://localhost:3000
dev:
	cd dagster_project && $(VENV)/dagster dev

## dashboard: Streamlit Customer 360 dashboard on http://localhost:8501
dashboard:
	mkdir -p .logs
	$(VENV)/streamlit run dashboard/app.py --server.port 8501 --server.address localhost

## verify: quick verification suite (queries only, no re-materialization)
verify:
	$(VENV)/python scripts/verify_lakehouse.py
	$(VENV)/python scripts/verify_bronze.py
	$(VENV)/python scripts/verify_silver.py
	$(VENV)/python scripts/verify_gold.py
	$(VENV)/python scripts/verify_dashboard.py

## verify-dq: heavy DQ suite (18 unit tests + negative test + full re-run)
verify-dq:
	$(VENV)/python scripts/verify_dq.py

## logs: tail all service logs
logs:
	docker compose -f docker/docker-compose.yml logs -f --tail=100

## clean: stop stack AND delete data volumes (full reset)
clean:
	docker compose -f docker/docker-compose.yml down -v
	rm -rf data/synthetic
