# Design and Trade-off Note

- **SQLite transaction:** target, event ledger, and watermark are committed together, enabling atomic rollback for this local single-writer scenario.
- **Event ledger:** primary key on event ID makes replay idempotent.
- **Tombstones:** deletion state remains in target and prevents an older upsert from resurrecting a row.
- **Late arrivals:** unseen events are processed regardless of watermark. The watermark is a checkpoint/metric, not a cutoff.
- **Deterministic conflict resolution:** order by source timestamp and event ID; only a newer event updates current state.
- **Quarantine:** invalid records remain traceable to source line and run.

Trade-offs: full input scans are correct but less efficient; SQLite is not distributed; event IDs must be stable; a full slowly-changing-dimension history table is not included. Production extensions could add schema versioning, structured log export/alerting, source CDC offsets plus periodic backfill, bulk inserts, and full point-in-time history.
