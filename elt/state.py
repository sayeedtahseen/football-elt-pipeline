"""Run + watermark tracking.

Phase 2: a ``football_raw._ingestion_runs`` table written at the start and
end of every extractor, so a failed 3am cron run is visible the next morning.
"""

from datetime import datetime, timezone
import json
import logging
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

from elt.bigquery import BigQueryRawLoader

log = logging.getLogger(__name__)

class IngestionRunTracker:
    TABLE = "_ingestion_runs"
    SCHEMA = [
        bigquery.SchemaField("run_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("endpoint", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("params", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("started_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("finished_at", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("rows_loaded", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("error_message", "STRING", mode="NULLABLE"),
    ]

    def __init__(self, loader: BigQueryRawLoader):
        self.loader = loader
        self.table_ref = loader.dataset_ref.table(self.TABLE)

    def ensure_table(self) -> None:
        try:
            self.loader.client.get_table(self.table_ref)
        except NotFound:
            table = bigquery.Table(self.table_ref, schema=self.SCHEMA)
            table.time_partitioning = bigquery.TimePartitioning(field="started_at")
            table.clustering_fields = ["run_id"]
            self.loader.client.create_table(table, exists_ok=True)
            log.info("created table %s", self.TABLE)

    def start_run(self, run_id: str, endpoint: str, params: dict) -> None:
        self.ensure_table()
        self._insert({
            "run_id": run_id,
            "endpoint": endpoint,
            "params": json.dumps(params, sort_keys=True) if params else None,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "finished_at": None,
            "status": "running",
            "rows_loaded": None,
            "error_message": None
            })

    def finish_run(self, run_id: str, endpoint: str, *, status: str, rows_loaded: int | None = None,
                   error_message: str | None = None) -> None:
        """Append the terminal row for this (run_id, endpoint). Option B: a second
        append, not an UPDATE -- readers take the latest row per (run_id, endpoint)."""

        if status not in ("success", "error"):
            raise ValueError(f"finish_run status must be 'success' or 'error', got {status!r}")
        self._insert(
            {
                "run_id": run_id,
                "endpoint": endpoint,
                "params": None,
                "started_at": datetime.now(timezone.utc).isoformat(),
                "finished_at": datetime.now(timezone.utc).isoformat(),
                "status": status,
                "rows_loaded": rows_loaded,
                "error_message": (error_message or "")[:1000] or None,
            })

    def _insert(self, row: dict) -> None:
        """Best-effort. Telemetry must never crash the pipeline."""
        try:
            errors = self.loader.client.insert_rows_json(self.table_ref, [row])
            if errors:
                log.warning("could not write %s row: %s", self.TABLE, errors)
        except Exception:  # noqa: BLE001 -- deliberately swallow all telemetry errors
            log.warning("could not write %s row", self.TABLE, exc_info=True)

    