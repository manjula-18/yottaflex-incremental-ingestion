# Data Dictionary

- Source JSONL grain: one row per source event.
- `event_id`: required stable event identifier; primary key in `processed_events`.
- `customer_id`: required business key; primary key in `customers`.
- `source_updated_at`: required timezone-aware ISO-8601 source event timestamp.
- `operation`: `upsert` or `delete`; delete is represented as a tombstone.
- `source_file`: lineage field supplied by source producer.
- `is_deleted`: 0 means active, 1 means tombstone.
- `pipeline_runs`: one row per execution attempt with counts, status, failure reason, and elapsed time.
- `quarantine`: rejected source line with run ID, file, line number, raw record, and reason.
- `pipeline_state.watermark`: maximum timestamp in processed event ledger; not a filter for late events.

Upsert requires a non-empty name and non-negative numeric amount. Email is nullable. Unknown columns are rejected to make schema drift visible. No foreign key is imposed on delete events because a deletion may arrive before the entity was ever observed.
