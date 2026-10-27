
import json
import math
import sqlite3
import time
import uuid
from pathlib import Path
from datetime import datetime, timezone


SCHEMA = """
CREATE TABLE IF NOT EXISTS customers (
    customer_id TEXT PRIMARY KEY,
    name TEXT,
    email TEXT,
    amount REAL,
    is_deleted INTEGER NOT NULL DEFAULT 0,
    source_updated_at TEXT NOT NULL,
    last_event_id TEXT NOT NULL,
    source_file TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS processed_events (
    event_id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    source_updated_at TEXT NOT NULL,
    operation TEXT NOT NULL,
    run_id TEXT NOT NULL,
    source_file TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pipeline_state (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

INSERT OR IGNORE INTO pipeline_state VALUES ('watermark', '');

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id TEXT PRIMARY KEY,
    source_file TEXT,
    started_at TEXT,
    ended_at TEXT,
    status TEXT,
    input_count INTEGER DEFAULT 0,
    accepted_count INTEGER DEFAULT 0,
    rejected_count INTEGER DEFAULT 0,
    applied_count INTEGER DEFAULT 0,
    duplicate_count INTEGER DEFAULT 0,
    error TEXT,
    elapsed_seconds REAL
);

CREATE TABLE IF NOT EXISTS quarantine (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT,
    source_file TEXT,
    line_number INTEGER,
    raw_record TEXT,
    reason TEXT,
    created_at TEXT
);
"""


def now():
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def ts(value):
    """Validate an ISO-8601 timestamp and normalize it to UTC."""
    if not isinstance(value, str) or not value.strip():
        raise ValueError("source_updated_at must be ISO-8601")

    value = value.strip()

    if value.endswith("Z"):
        value = value[:-1] + "+00:00"

    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        raise ValueError("source_updated_at must be valid ISO-8601")

    if parsed.tzinfo is None:
        raise ValueError(
            "source_updated_at must include timezone, e.g. Z"
        )

    return (
        parsed.astimezone(timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )


def ts_key(value):
    """Return a datetime for safe chronological comparisons."""
    normalized = ts(value)
    return datetime.fromisoformat(
        normalized.replace("Z", "+00:00")
    )


def validate(record):
    if not isinstance(record, dict):
        raise ValueError("record must be a JSON object")

    required = {
        "event_id",
        "customer_id",
        "source_updated_at",
        "operation",
        "source_file",
    }

    missing = required - record.keys()

    if missing:
        raise ValueError(
            "missing required field(s): "
            + ", ".join(sorted(missing))
        )

    allowed = required | {"name", "email", "amount"}
    unknown = set(record) - allowed

    if unknown:
        raise ValueError(
            "unexpected schema field(s): "
            + ", ".join(sorted(unknown))
        )

    for field in (
        "event_id",
        "customer_id",
        "operation",
        "source_file",
    ):
        if (
            not isinstance(record[field], str)
            or not record[field].strip()
        ):
            raise ValueError(
                f"{field} must be a non-empty string"
            )

    operation = record["operation"].strip().lower()

    if operation not in ("upsert", "delete"):
        raise ValueError(
            "operation must be 'upsert' or 'delete'"
        )

    amount = record.get("amount")
    email = record.get("email")

    if operation == "upsert":
        name = record.get("name")

        if not isinstance(name, str) or not name.strip():
            raise ValueError("name required for upsert")

        if (
            isinstance(amount, bool)
            or not isinstance(amount, (int, float))
            or not math.isfinite(amount)
            or amount < 0
        ):
            raise ValueError(
                "amount must be a non-negative number"
            )

    elif amount is not None:
        if (
            isinstance(amount, bool)
            or not isinstance(amount, (int, float))
            or not math.isfinite(amount)
            or amount < 0
        ):
            raise ValueError("amount must be non-negative")

    if email is not None and not isinstance(email, str):
        raise ValueError("email must be string or null")

    return {
        "event_id": record["event_id"].strip(),
        "customer_id": record["customer_id"].strip(),
        "name": record.get("name"),
        "email": email,
        "amount": float(amount) if amount is not None else None,
        "source_updated_at": ts(record["source_updated_at"]),
        "operation": operation,
        "source_file": record["source_file"].strip(),
    }


def connect(db):
    path = Path(db)
    path.parent.mkdir(parents=True, exist_ok=True)

    connection = sqlite3.connect(path, timeout=30)
    connection.row_factory = sqlite3.Row
    connection.executescript(SCHEMA)

    return connection


def run_pipeline(input_path, db_path, fail_after=None):
    started = time.perf_counter()
    run_id = str(uuid.uuid4())
    source = Path(input_path).name
    start_time = now()

    connection = connect(db_path)

    connection.execute(
        """
        INSERT INTO pipeline_runs
        (run_id, source_file, started_at, status)
        VALUES (?, ?, ?, ?)
        """,
        (run_id, source, start_time, "running"),
    )
    connection.commit()

    input_count = 0
    accepted = 0
    rejected = 0
    applied = 0
    duplicates = 0

    rejects = []
    good_records = []
    error = None
    status = "success"

    try:
        # Read and validate every non-empty input line.
        with open(input_path, encoding="utf-8") as file:
            for line_number, line in enumerate(file, 1):
                if not line.strip():
                    continue

                input_count += 1

                try:
                    raw_record = json.loads(line)
                    record = validate(raw_record)

                    good_records.append(
                        (
                            line_number,
                            record,
                            line.rstrip("\n"),
                        )
                    )

                except Exception as exc:
                    rejects.append(
                        (
                            line_number,
                            line.rstrip("\n"),
                            str(exc),
                        )
                    )

        # Reject repeated event IDs within this batch.
        seen_event_ids = set()
        unique_records = []

        for line_number, record, raw_record in good_records:
            event_id = record["event_id"]

            if event_id in seen_event_ids:
                rejects.append(
                    (
                        line_number,
                        raw_record,
                        "duplicate event_id within batch: "
                        + event_id,
                    )
                )
                continue

            seen_event_ids.add(event_id)
            unique_records.append(
                (line_number, record, raw_record)
            )

        accepted = len(unique_records)
        rejected = len(rejects)

        # Sort chronologically using datetime objects, not strings.
        unique_records.sort(
            key=lambda item: (
                ts_key(item[1]["source_updated_at"]),
                item[1]["event_id"],
            )
        )

        # Target changes and event tracking share one transaction.
        connection.execute("BEGIN")

        for line_number, record, raw_record in unique_records:
            event_id = record["event_id"]

            already_processed = connection.execute(
                "SELECT 1 FROM processed_events WHERE event_id = ?",
                (event_id,),
            ).fetchone()

            if already_processed:
                duplicates += 1
                continue

            current = connection.execute(
                """
                SELECT source_updated_at, last_event_id
                FROM customers
                WHERE customer_id = ?
                """,
                (record["customer_id"],),
            ).fetchone()

            incoming_key = (
                ts_key(record["source_updated_at"]),
                record["event_id"],
            )

            current_key = None

            if current is not None:
                current_key = (
                    ts_key(current["source_updated_at"]),
                    current["last_event_id"],
                )

            should_apply = (
                current is None or incoming_key > current_key
            )

            connection.execute(
                """
                INSERT INTO processed_events
                (
                    event_id,
                    customer_id,
                    source_updated_at,
                    operation,
                    run_id,
                    source_file
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    record["event_id"],
                    record["customer_id"],
                    record["source_updated_at"],
                    record["operation"],
                    run_id,
                    record["source_file"],
                ),
            )

            if should_apply:
                deleted = int(record["operation"] == "delete")

                values = (
                    record["customer_id"],
                    None if deleted else record["name"],
                    None if deleted else record["email"],
                    None if deleted else record["amount"],
                    deleted,
                    record["source_updated_at"],
                    record["event_id"],
                    record["source_file"],
                )

                connection.execute(
                    """
                    INSERT INTO customers
                    (
                        customer_id,
                        name,
                        email,
                        amount,
                        is_deleted,
                        source_updated_at,
                        last_event_id,
                        source_file
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(customer_id) DO UPDATE SET
                        name = excluded.name,
                        email = excluded.email,
                        amount = excluded.amount,
                        is_deleted = excluded.is_deleted,
                        source_updated_at = excluded.source_updated_at,
                        last_event_id = excluded.last_event_id,
                        source_file = excluded.source_file
                    """,
                    values,
                )

                applied += 1

                # Test failure recovery without partially publishing data.
                if (
                    fail_after is not None
                    and applied >= fail_after
                ):
                    raise RuntimeError(
                        "injected failure after "
                        f"{applied} applied record(s)"
                    )

        # Calculate the watermark chronologically, not lexically.
        timestamp_rows = connection.execute(
            """
            SELECT source_updated_at
            FROM processed_events
            """
        ).fetchall()

        max_timestamp = max(
            (row["source_updated_at"] for row in timestamp_rows),
            key=ts_key,
            default=None,
        )

        if max_timestamp is not None:
            connection.execute(
                """
                UPDATE pipeline_state
                SET value = ?
                WHERE key = 'watermark'
                """,
                (max_timestamp,),
            )

        connection.commit()

    except Exception as exc:
        connection.rollback()

        status = "failed"
        error = f"{type(exc).__name__}: {exc}"

        # The transaction rolled back, so none of its changes persisted.
        applied = 0
        duplicates = 0

    # Preserve quarantine entries and run status even if processing failed.
    for line_number, raw_record, reason in rejects:
        connection.execute(
            """
            INSERT INTO quarantine
            (
                run_id,
                source_file,
                line_number,
                raw_record,
                reason,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                source,
                line_number,
                raw_record,
                reason,
                now(),
            ),
        )

    elapsed = round(time.perf_counter() - started, 6)

    connection.execute(
        """
        UPDATE pipeline_runs
        SET ended_at = ?,
            status = ?,
            input_count = ?,
            accepted_count = ?,
            rejected_count = ?,
            applied_count = ?,
            duplicate_count = ?,
            error = ?,
            elapsed_seconds = ?
        WHERE run_id = ?
        """,
        (
            now(),
            status,
            input_count,
            accepted,
            rejected,
            applied,
            duplicates,
            error,
            elapsed,
            run_id,
        ),
    )
    connection.commit()

    watermark = connection.execute(
        """
        SELECT value
        FROM pipeline_state
        WHERE key = 'watermark'
        """
    ).fetchone()[0]

    active_count = connection.execute(
        """
        SELECT COUNT(*)
        FROM customers
        WHERE is_deleted = 0
        """
    ).fetchone()[0]

    connection.close()

    return {
        "run_id": run_id,
        "status": status,
        "source_file": source,
        "input_count": input_count,
        "accepted_count": accepted,
        "rejected_count": rejected,
        "applied_count": applied,
        "duplicate_count": duplicates,
        "active_target_count": active_count,
        "watermark": watermark,
        "elapsed_seconds": elapsed,
        "events_per_second": (
            round(input_count / elapsed, 2)
            if elapsed
            else None
        ),
        "error": error,
    }
