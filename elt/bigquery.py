"""BigQuery raw-layer loads.

Phase 2: idempotent dataset/table ensure with the fixed raw schema, plus
``append_rows`` using ``load_table_from_json`` with WRITE_APPEND and an
explicit schema (never autodetect).
"""
import logging

from google.cloud import bigquery
from google.api_core.exceptions import NotFound

from elt.config import Settings

log = logging.getLogger(__name__)

RAW_SCHEMA = [
    bigquery.SchemaField("run_id",          "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("ingested_at",     "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("source_endpoint", "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("request_params",  "STRING"),
    bigquery.SchemaField("payload",         "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("record_hash",     "STRING",    mode="REQUIRED"),
]

class BigQueryRawLoader:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.client = bigquery.Client(project=settings.gcp_project, location=settings.bq_location)
        self.dataset_ref = bigquery.DatasetReference(settings.gcp_project, settings.bq_raw_dataset)


    def ensure_dataset(self) -> None:
        """Create the raw dataset if it does not exist."""
        try:
            self.client.get_dataset(self.dataset_ref)
        except NotFound:
            ds = bigquery.Dataset(self.dataset_ref)
            ds.location = self.settings.bq_location
            self.client.create_dataset(ds, exists_ok=True)
            log.info("created dataset %s in %s", self.settings.bq_raw_dataset, self.settings.bq_location)


    def ensure_table(self, name: str) -> bigquery.Table:
        """Create a raw_<endpoint> table with the fixed schema if it does not exist."""
        table_ref = self.dataset_ref.table(name)
        try:
            return self.client.get_table(table_ref)
        except NotFound:
            table = bigquery.Table(table_ref, schema=RAW_SCHEMA)
            table.time_partitioning = bigquery.TimePartitioning(field="ingested_at")   # DATE(ingested_at)
            table.clustering_fields = ["source_endpoint"]
            created = self.client.create_table(table, exists_ok=True)
            log.info("created table %s", name)
            return created


    def append_rows(self, table: str, rows: list[dict]) -> int:
        """Append rows to a raw table (append-only, explicit schema)."""
        if not rows:
            return 0

        self.ensure_dataset()
        self.ensure_table(table)

        job_config = bigquery.LoadJobConfig(
            schema=RAW_SCHEMA,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        )

        job = self.client.load_table_from_json(
            rows,
            self.dataset_ref.table(table),
            job_config=job_config,
        )

        job.result()
        return len(rows)
