# Incremental Data Ingestion Framework

Local Data Engineering challenge project demonstrating incremental ingestion, event-time watermarks, late arrivals, idempotent replay, tombstones, validation/quarantine, run audit, rollback/retry, reconciliation, and reproducible synthetic data.

## Architecture

```text
JSONL source -> parse/schema + business validation -> quarantine invalid rows
            -> deterministic event ordering -> SQLite transaction
            -> processed-event ledger + current-state target + watermark
            -> run metrics and reconciliation report
```

SQLite makes the project runnable locally without cloud credentials. Target updates, processed-event ledger entries, and watermark advancement commit together. Failure injection demonstrates rollback and retry.

## Schema and rules

Each JSONL line is an event with `event_id` (stable unique event ID), `customer_id` (business key), `source_updated_at` (timezone-aware ISO-8601), `operation` (`upsert` or `delete`), and `source_file`. Upserts also require non-empty `name` and non-negative numeric `amount`; `email` may be null. Unknown fields, malformed JSON, missing required fields, invalid timestamps, and invalid operations are quarantined with the raw line, source file, line number, run ID, and reason.

Tables: `customers` (one current-state row per customer, including tombstones), `processed_events` (event ID ledger), `pipeline_state` (watermark), `pipeline_runs` (execution metrics/status), and `quarantine` (rejected records). See `docs/data_dictionary.md`.

**Late arrivals:** the watermark is the maximum processed source timestamp, but is not used to discard older events. Every input batch is scanned and previously unseen event IDs are considered. A late event is recorded in the ledger but does not overwrite newer current state. This favors correctness over scan efficiency.

**Idempotency and recovery:** duplicate event IDs are only applied once. Target, event ledger, and watermark use one SQLite transaction. If processing fails before commit, these changes roll back; run status records the failure. Retry with the same input is safe. Deletes are tombstones so older upserts cannot resurrect deleted records.

## Setup (Windows PowerShell)

Requires Python 3.10+.

```powershell
cd yottaflex_incremental_ingestion
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python -m pytest -q
```

If PowerShell blocks activation, use `.\.venv\Scripts\python.exe` in place of `python`.

## Run

```powershell
python -m ingestion.generate_data --output data/sample_events.jsonl --records 30 --seed 42
python -m ingestion.cli --input data/sample_events.jsonl --db outputs/ingestion.db
python -m ingestion.report --db outputs/ingestion.db
```

Scale test, where practical:

```powershell
python -m ingestion.generate_data --output data/large_events.jsonl --records 100000 --seed 42
python -m ingestion.cli --input data/large_events.jsonl --db outputs/large.db
python -m ingestion.report --db outputs/large.db
```

Demonstrate rollback and retry (the first command intentionally exits with failure):

```powershell
python -m ingestion.cli --input data/sample_events.jsonl --db outputs/retry.db --fail-after 5
python -m ingestion.cli --input data/sample_events.jsonl --db outputs/retry.db
python -m ingestion.report --db outputs/retry.db
```

Output: `outputs/last_run.json` and `outputs/report.json`, plus the SQLite DB. CLI reports elapsed seconds and input events/second. The generator uses a fixed seed and includes null email values, deletes, invalid rows, a duplicate event, and malformed JSON.

## Tests and acceptance evidence

Run `python -m pytest -q`. Tests cover happy path, empty batch, malformed JSON, missing fields, null required fields, duplicate IDs, unexpected schema, invalid operation, negative amount, late arrivals, tombstones, reruns, and injected partial failure with retry.

## Performance and limitations

A reasonable optimization is to use a source change feed/offset or bounded lookback with periodic backfill, plus batch inserts. Full-file scanning is intentionally used here to ensure late events are not silently lost. SQLite is a local single-writer solution, not a distributed production ingestion service. Event IDs must be stable and unique at source. This project stores current state plus an event ledger, not full row-version history.

## Before submission

1. Run tests and the demo on your laptop; inspect the generated report.
2. Keep `docs/prompt_history.md` updated with the exact prompts you actually use with AI tools; do not invent entries.
3. Review and understand the code, especially transaction boundaries, watermark policy, event ledger, and tombstones.
4. Push the project to a **public** GitHub repository and verify the URL in a private/incognito window.
5. Submit only after checking all deliverables; the portal allows one submission.
