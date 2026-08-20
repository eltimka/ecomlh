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

.PHONY: run up down bootstrap refresh dev dashboard reseed verify verify-dq maintain logs clean stream-up stream-down flink-up flink-down

## run: one-command end-to-end startup (compose up -> data -> bootstrap ->
##      full refresh with all checks -> quick verification suite)
run:
	$(VENV)/python scripts/start_all.py

## up: start the docker stack and wait until healthy
up:
	docker compose -f docker/docker-compose.yml up -d
	docker compose -f docker/docker-compose.yml ps

## down: stop the docker stack, the dashboard and the stream producer
##      (data volumes are kept)
down:
	docker compose -f docker/docker-compose.yml down
	@if [ -f .logs/dashboard.pid ]; then kill `cat .logs/dashboard.pid` 2>/dev/null; rm -f .logs/dashboard.pid; echo "dashboard stopped"; fi
	@if [ -f .logs/stream_producer.pid ]; then kill `cat .logs/stream_producer.pid` 2>/dev/null; rm -f .logs/stream_producer.pid; echo "stream producer stopped"; fi

## bootstrap: MinIO buckets + Trino schemas (idempotent)
bootstrap:
	$(VENV)/python scripts/bootstrap_minio.py

## refresh: materialize bronze -> silver -> gold with all 92 DQ checks
refresh:
	cd dagster_project && $(VENV)/dagster job execute -m ecommerce_lakehouse.definitions -j lakehouse_refresh

## reseed: regenerate synthetic data with a new seed and re-run the pipeline
## (batch + stream). Resets the Flink job + stream table + Kafka topic.
## usage: make reseed SEED=123   (SEED=42 restores the canonical dataset)
reseed:
	@test -n "$(SEED)" || { echo "usage: make reseed SEED=<n>   (42 = canonical dataset)"; exit 1; }
	rm -rf data/synthetic
	$(VENV)/python data_generator/generate_synthetic.py --seed $(SEED)
	$(MAKE) flink-down
	$(VENV)/python scripts/reset_stream.py
	$(VENV)/python data_generator/stream_producer.py --mode replay --reset-topic
	# silver's fct_web_events merges the stream table, so the stream must be
	# re-submitted AND caught up before the refresh (verify_stream polls)
	$(MAKE) flink-up
	$(VENV)/python scripts/verify_stream.py
	$(MAKE) refresh
	$(MAKE) verify

# job name of the Flink streaming job (see flink/sql/stream_web_events.sql)
STREAM_JOB := insert-into_iceberg.bronze.stream_web_events

## flink-up: submit the Kafka -> Iceberg stream job if it is not already running
flink-up:
	@JID=$$(curl -s http://localhost:8081/jobs/overview 2>/dev/null | $(VENV)/python -c 'import json,sys; jobs=json.load(sys.stdin).get("jobs",[]); m=[j["jid"] for j in jobs if j["name"]=="'"$(STREAM_JOB)"'" and j["state"] in ("RUNNING","RESTARTING")]; print(m[0] if m else "")'); \
	if [ -n "$$JID" ]; then \
		echo "stream job already running ($$JID)"; \
	else \
		docker compose -f docker/docker-compose.yml exec -T flink-jobmanager \
			./bin/sql-client.sh -f /opt/flink/sql/stream_web_events.sql; \
		echo "stream job submitted"; \
	fi

## flink-down: cancel the Kafka -> Iceberg stream job (containers stay up)
flink-down:
	@JID=$$(curl -s http://localhost:8081/jobs/overview 2>/dev/null | $(VENV)/python -c 'import json,sys; jobs=json.load(sys.stdin).get("jobs",[]); m=[j["jid"] for j in jobs if j["name"]=="'"$(STREAM_JOB)"'" and j["state"] in ("RUNNING","RESTARTING")]; print(m[0] if m else "")'); \
	if [ -n "$$JID" ]; then \
		docker exec flink-jobmanager ./bin/flink cancel $$JID; \
	else \
		echo "stream job not running"; \
	fi

## stream-up: start the live streaming producer in the background (demo mode;
##            replays the history, then emits new events on a simulated clock)
stream-up:
	@mkdir -p .logs
	@if [ -f .logs/stream_producer.pid ] && kill -0 $$(cat .logs/stream_producer.pid) 2>/dev/null; then \
		echo "stream producer already running (pid $$(cat .logs/stream_producer.pid))"; exit 1; fi
	@( nohup $(VENV)/python data_generator/stream_producer.py --mode live --if-empty >> .logs/stream_producer.log 2>&1 & echo $$! > .logs/stream_producer.pid )
	@echo "stream producer started (live mode, history preamble only if the topic is empty); log: .logs/stream_producer.log"

## stream-down: stop the live streaming producer
stream-down:
	@if [ -f .logs/stream_producer.pid ]; then \
		kill $$(cat .logs/stream_producer.pid) 2>/dev/null; rm -f .logs/stream_producer.pid; \
		echo "stream producer stopped"; \
	else \
		echo "stream producer not running"; \
	fi

## dev: Dagster UI on http://localhost:3000
dev:
	cd dagster_project && $(VENV)/dagster dev

## dashboard: Streamlit Customer 360 dashboard on http://localhost:8501
dashboard:
	mkdir -p .logs
	$(VENV)/streamlit run dashboard/app.py --server.port 8501 --server.address localhost --server.headless true

## verify: quick verification suite (queries only, no re-materialization)
verify:
	$(VENV)/python scripts/verify_lakehouse.py
	$(VENV)/python scripts/verify_kafka.py
	$(VENV)/python scripts/verify_stream.py
	$(VENV)/python scripts/verify_bronze.py
	$(VENV)/python scripts/verify_silver.py
	$(VENV)/python scripts/verify_gold.py
	$(VENV)/python scripts/verify_dashboard.py

## verify-dq: heavy DQ suite (27 unit tests + negative test + full re-run)
verify-dq:
	$(VENV)/python scripts/verify_dq.py

## maintain: Iceberg maintenance - expire old snapshots + remove orphan files
maintain:
	$(VENV)/python scripts/maintain_iceberg.py

## logs: tail all service logs
logs:
	docker compose -f docker/docker-compose.yml logs -f --tail=100

## clean: stop stack AND delete data volumes (full reset)
clean:
	docker compose -f docker/docker-compose.yml down -v
	rm -rf data/synthetic
