"""BigQuery raw-layer loads.

Phase 2: idempotent dataset/table ensure with the fixed raw schema, plus
``append_rows`` using ``load_table_from_json`` with WRITE_APPEND and an
explicit schema (never autodetect).
"""


def ensure_dataset():
    """Create the raw dataset if it does not exist."""
    raise NotImplementedError


def ensure_table(name):
    """Create a raw_<endpoint> table with the fixed schema if it does not exist."""
    raise NotImplementedError


def append_rows(table, rows):
    """Append rows to a raw table (append-only, explicit schema)."""
    raise NotImplementedError
