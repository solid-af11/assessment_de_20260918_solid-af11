# Notes

## Time spent

Roughly how many hours, and how it was split (setup / extract-load / dbt / airflow / notebook).

- Setup (WSL/Docker environment, fixing missing Dockerfile, database/permission issues): ~2 hours
- Extract-load (ingestion/extract.py, ingestion/load.py, ingestion/run.py): ~1.5 hours
- dbt (staging model, mart, schema tests, schema-naming fix, permission debugging): ~1.5 hours
- Airflow (DAG, XCom wiring, PYTHONPATH fix, dags test run): ~1 hour
- Notebook (walkthrough.ipynb, re-run safety proof, make reproduce verification): ~1 hour

Total: ~7 hours

## What I would do with more time

- Automate the missing `airflow` metadata database creation on first boot (e.g. an init
  container or entrypoint script that runs `CREATE DATABASE IF NOT EXISTS airflow`), so a
  clean `make up` never requires the manual fix I had to apply.
- Fix the recurring host/container file-ownership mismatch (files created inside containers
  ending up owned by root or by mismatched UIDs, blocking writes from both the host user and
  from other containers like `dbt deps` failing to write `dbt_packages/`). Would add this as
  a documented one-time `chmod`/`chown` step in the README, or better, align the container
  user's UID with the host user at build time.
- Add more cities and a longer backfill window to better exercise the mart's aggregation
  logic across a real date range, not just a single logical date per demo run.
- Add a second mart or a rolling-average model (e.g. 7-day trailing average temperature per
  city) to show a slightly richer transformation than a 1:1 staging-to-mart mapping.
- Add CI (GitHub Actions) to run `make up` + `make reproduce` on every push, catching
  environment or pipeline regressions automatically instead of relying on manual verification.
- Split `extract` and `load` into fully independent Airflow tasks with their own per-city
  dynamic task mapping, rather than looping over cities inside each task — would give
  finer-grained retry/observability per city.

## Known gaps

- Only 4 cities configured in `config/cities.yml` (London, New York, Tokyo, Mumbai) — easy to
  extend, just adds more API calls.
- Single mart (`fct_city_daily`); no second mart or cross-city aggregation model.
- No CI/automated testing beyond `make reproduce` run manually before submission.
- The `airflow` metadata database is not auto-created — see setup issues below. A completely
  fresh clone will need this manual step once (documented here and would be automated with
  more time).
- Notebook uses one fixed `TARGET_DATE` for demonstration rather than looping over the full
  30-day range the task describes; the DAG and `run_for_date_range` helper support backfill
  over any range, but the notebook only walks through a single day for readability.

## Setup issues encountered (environment, not pipeline logic)

- `docker-compose.yml` referenced `docker/airflow.Dockerfile` for both `airflow` and
  `jupyter` services, but the `docker/` folder was missing from the repo scaffold entirely.
  Recreated it (based on `apache/airflow:2.10.2-python3.11`, installing `requirements.txt`).
- After the image built, `airflow` failed to start with `FATAL: database "airflow" does not
  exist` — Postgres only auto-creates the database named in `POSTGRES_DB` (`warehouse`), not
  the separate `airflow` metadata DB Airflow itself needs. Fixed by manually running
  `CREATE DATABASE airflow;` inside the postgres container, then restarting airflow.
- `dbt deps` was silently failing (exit code 2, no visible output, no log file created) —
  root cause was a UID mismatch: the airflow container's user is `uid=50000`, but the
  host-mounted `dbt/` folder was owned by `1000:1000`, so the container user couldn't write
  `dbt_packages/` or `logs/`. Fixed with `chmod -R a+w dbt/` (and later applied the same fix
  to other mounted folders as needed).
- dbt's default schema behaviour prepends the profile's target schema, so `+schema: marts`
  in `dbt_project.yml` produced `public_marts` instead of `marts`. Added a custom
  `generate_schema_name` macro so schemas match the task spec exactly
  (`raw`, `staging`, `marts`).
- The Airflow DAG failed to import with `ModuleNotFoundError: No module named 'ingestion'`
  because `/opt/airflow` wasn't on `PYTHONPATH`. Added `PYTHONPATH: /opt/airflow` to the
  shared environment block in `docker-compose.yml` so both `airflow` and `jupyter` can
  `import ingestion`.

## AI-usage declaration

| Where (file / area) | What the tool did | What I changed afterwards |
|---|---|---|
| Environment/Docker setup | Asked Claude how to diagnose `docker-compose.yml` referencing a missing `docker/airflow.Dockerfile`. | Wrote and verified the Dockerfile, ran `make up`, and debugged the resulting `FATAL: database "airflow" does not exist` error myself using the traceback, with Claude helping interpret the psycopg2/SQLAlchemy stack trace. |
| Postgres/Airflow DB fix | Asked how to fix the missing `airflow` metadata database once I'd identified the root cause from the traceback. | Ran the `CREATE DATABASE airflow;` command myself, confirmed via logs that migrations completed and the webserver started. |
| `dbt deps` permission failure | Asked Claude to help debug why `dbt deps` failed silently (exit code 2, no output). It walked me through isolating the cause to a UID mismatch (`airflow` container user `50000` vs host-owned `dbt/` folder `1000:1000`). | Applied the `chmod -R a+w dbt/` fix myself and confirmed `dbt deps`/`dbt run`/`dbt test` worked afterward. |
| `ingestion/extract.py`, `ingestion/load.py` | Discussed the `requests` + `tenacity` retry/timeout approach for the Open-Meteo API call, and the delete+insert pattern for idempotent loads, after I decided delete+insert (scoped to city+date) was the re-run-safety mechanism to use. | Ran the extract/load against the live API and Postgres myself, verified row counts, and proved re-run safety by loading the same date twice and checking counts stayed constant. |
| `ingestion/run.py` | Discussed the shape of a single entrypoint function combining extract+load per city, reading from `config/cities.yml`, so the same function could be reused by both the DAG and the notebook. | Tested `python -m ingestion.run <date>` directly, confirmed output row counts per city, and confirmed no code duplication between DAG/notebook by having both import from this file. |
| `dbt/models/staging/stg_weather.sql`, `fct_city_daily.sql`, schema YAML | Discussed the staging/mart structure and the `not_null`/`accepted_range`/`unique_combination_of_columns` schema tests, based on the columns I specified from the raw API fields. | Ran `dbt run` and `dbt test` myself, inspected the actual mart output (`SELECT * FROM marts.fct_city_daily`) for realistic values, and diagnosed and fixed the schema-naming issue (`public_marts` vs `marts`) myself, with Claude providing the `generate_schema_name` macro to fix it. |
| `dags/weather_pipeline_dag.py` | Discussed a DAG structure matching the required `extract → load → dbt run → dbt test` stages, using `logical_date` (not hardcoded dates) so backfill would work, with XCom to pass records between extract and load tasks. | Fixed a `ModuleNotFoundError` for the `ingestion` package myself (added `PYTHONPATH` to `docker-compose.yml`), then verified the DAG with `airflow dags list-import-errors` and `airflow dags test weather_pipeline <date>`, confirming all four tasks succeeded and data landed correctly. |
| `notebooks/walkthrough.ipynb` | Discussed the cell structure (markdown explanations + code cells) to walk through each stage in order, calling the same `ingestion`/dbt code as the DAG rather than reimplementing logic. | Ran every cell myself, fixed a pandas/SQLAlchemy warning by switching the query connection, verified the re-run safety proof and the mart query output, and confirmed `make reproduce` executes the notebook headlessly end-to-end before committing it with outputs. |

I did not have AI design the pipeline architecture, choose the re-run-safety mechanism, or
generate dbt test results / DAG run results / notebook outputs — those are all from my own
executions against the live pipeline.
