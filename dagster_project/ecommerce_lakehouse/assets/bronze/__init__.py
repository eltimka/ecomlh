"""Bronze layer (raw ingested data) - asset group.

Each asset ingests one Phase-4 Parquet file (data/synthetic/<table>.parquet)
into the lakehouse:

1. Uploads the raw file to the immutable landing zone
   ``s3a://bronze/raw/<table>/<table>.parquet`` (MinIO, via MinioResource).
2. Registers it as a Trino hive external table ``hive.bronze_raw.<table>``
   (Trino's native S3 filesystem, no Hadoop).
3. Full-refresh loads it into Iceberg ``iceberg.bronze.<table>`` via CTAS.

Full-refresh semantics: re-materializing an asset replaces the Iceberg table
(drop + create). Raw files are keyed by table name, so re-runs overwrite the
same object - with the deterministic Phase-4 generator that is byte-stable.
(An append-only variant would key raw files by run timestamp instead.)
"""

import os
from pathlib import Path

import dagster as dg
import polars as pl
from dagster import AssetExecutionContext

from ..factory import GATE_NAME, build_layer_gate
from ...resources.minio import MinioResource
from ...resources.trino import TrinoResource

# assets/bronze/ -> assets/ -> ecommerce_lakehouse/ -> dagster_project/ -> repo root
REPO_ROOT = Path(__file__).resolve().parents[4]

#: Source tables produced by data_generator/generate_synthetic.py (Phase 4),
#: in ingestion order (customers/products before their dependents - the load
#: itself is independent per table; order only documents the relationships).
BRONZE_TABLES: list[str] = [
    "customers",
    "products",
    "orders",
    "order_items",
    "payments",
    "refunds",
    "web_events",
    "support_tickets",
]


def _source_dir() -> Path:
    """Directory with the Phase-4 Parquet files (.env: SYNTHETIC_DATA_DIR)."""
    return Path(os.environ.get("SYNTHETIC_DATA_DIR", str(REPO_ROOT / "data" / "synthetic")))


def _sql_type(dtype: pl.DataType) -> str:
    """Map a polars parquet dtype to the Trino/Iceberg DDL type."""
    if isinstance(dtype, pl.Utf8):
        return "varchar"
    if isinstance(dtype, pl.Boolean):
        return "boolean"
    if isinstance(dtype, pl.Int8) or isinstance(dtype, pl.Int16) or isinstance(dtype, pl.Int32):
        return "integer"
    if isinstance(dtype, pl.Int64):
        return "bigint"
    if isinstance(dtype, pl.Float32):
        return "real"
    if isinstance(dtype, pl.Float64):
        return "double"
    if isinstance(dtype, pl.Date):
        return "date"
    if isinstance(dtype, pl.Datetime):
        return "timestamp"
    raise ValueError(f"Unsupported bronze dtype for {dtype!r} - extend _sql_type")


def _ingest_table(
    context: AssetExecutionContext,
    table: str,
    minio: MinioResource,
    trino: TrinoResource,
) -> None:
    """Run the bronze ingestion flow for one table; report metadata."""
    bucket = minio.bucket_bronze

    src = _source_dir() / f"{table}.parquet"
    if not src.exists():
        raise dg.Failure(
            f"Source file {src} not found. Run the Phase 4 generator first: "
            "python data_generator/generate_synthetic.py"
        )

    # 1. Raw landing zone (immutable copy of the raw artifact)
    raw_key = f"raw/{table}/{table}.parquet"
    raw_uri = minio.upload_file(src, raw_key, bucket=bucket)

    # 2. Hive external table over the raw directory (schema inferred from
    #    the actual parquet, so the DDL always matches the data)
    schema = pl.read_parquet_schema(src)
    columns = ", ".join(f"{name} {_sql_type(dtype)}" for name, dtype in schema.items())
    trino.execute("CREATE SCHEMA IF NOT EXISTS hive.bronze_raw")
    trino.execute(f"DROP TABLE IF EXISTS hive.bronze_raw.{table}")
    trino.execute(
        f"CREATE TABLE hive.bronze_raw.{table} ({columns}) "
        f"WITH (format='PARQUET', external_location='s3a://{bucket}/raw/{table}/')"
    )

    # 3. Full-refresh load into Iceberg
    trino.execute(f"DROP TABLE IF EXISTS iceberg.bronze.{table}")
    trino.execute(
        f"CREATE TABLE iceberg.bronze.{table} "
        f"WITH (format='PARQUET', location='s3a://{bucket}/{table}') "
        f"AS SELECT * FROM hive.bronze_raw.{table}"
    )

    (row_count,) = trino.fetch(f"SELECT count(*) FROM iceberg.bronze.{table}")[0]
    context.add_output_metadata(
        {
            "table": f"iceberg.bronze.{table}",
            "rows": row_count,
            "raw_location": f"s3a://{bucket}/raw/{table}/",
            "source": str(src),
            "raw_size_bytes": int(src.stat().st_size),
        }
    )
    context.log.info(
        f"bronze.{table}: {row_count} rows -> iceberg.bronze.{table} "
        f"(raw: {raw_uri})"
    )


def _bronze_asset(table: str):
    """Build the Dagster asset that ingests one bronze table."""

    @dg.asset(
        key_prefix=("bronze",),
        name=table,
        description=(
            f"Bronze ingestion for `{table}`: uploads data/synthetic/{table}.parquet "
            f"to s3a://bronze/raw/{table}/ and full-refresh loads "
            f"iceberg.bronze.{table} via Trino CTAS. Idempotent."
        ),
    )
    def bronze_asset(
        context: AssetExecutionContext, minio: MinioResource, trino: TrinoResource
    ) -> None:
        _ingest_table(context, table, minio, trino)

    return bronze_asset


bronze_assets: list = [_bronze_asset(table) for table in BRONZE_TABLES]


def _stream_asset(name: str, pk: str, date_col: str | None):
    """Virtual asset for one ``iceberg.bronze.stream_*`` table.

    The table is written by a Flink job (Phases 11-13 web events, Phase 15
    item 4 money path), not by Dagster. The asset writes nothing - its op
    observes the table through Trino (existence, row count, distinct pk,
    date range) and reports it as run metadata, so the stream source shows
    up in the asset graph and silver's merge has a real upstream to depend
    on. Fails with the actionable fix (``make flink-up``) if the table is
    missing, because the silver merge reads it unconditionally.
    """

    fq = f"iceberg.bronze.{name}"

    @dg.asset(
        key_prefix=("bronze",),
        name=name,
        description=(
            f"Virtual observer for bronze.{name}: the table is written by "
            "the Flink job (append-only), this op only queries its state "
            "through Trino. Fails until the stream job has created the "
            "table (make flink-up)."
        ),
    )
    def stream_asset(context: AssetExecutionContext, trino: TrinoResource) -> None:
        (exists,) = trino.fetch(
            "SELECT count(*) FROM iceberg.information_schema.tables "
            "WHERE table_catalog = 'iceberg' AND table_schema = 'bronze' "
            f"AND table_name = '{name}'"
        )[0]
        if not int(exists):
            raise dg.Failure(
                f"{fq} does not exist - the Flink stream job has never run. "
                "Submit it with `make flink-up` (it creates the table and "
                "ingests the topic)."
            )
        (rows,) = trino.fetch(f"SELECT count(*) FROM {fq}")[0]
        meta: dict[str, int | str] = {
            "table": fq,
            "rows": int(rows),
            "written_by": "flink (make flink-up)",
        }
        if int(rows) > 0:
            (distinct,) = trino.fetch(f"SELECT count(DISTINCT {pk}) FROM {fq}")[0]
            meta[f"distinct_{pk}"] = int(distinct)
            if date_col:
                (mn, mx) = trino.fetch(f"SELECT min({date_col}), max({date_col}) FROM {fq}")[0]
                meta[f"min_{date_col}"] = str(mn)
                meta[f"max_{date_col}"] = str(mx)
        else:
            meta["note"] = "0 rows committed yet (first checkpoint pending)"
        context.add_output_metadata(meta)
        context.log.info(f"bronze.{name} (virtual): {meta}")

    return stream_asset


#: (stream table name, primary key, date column or None) - one per Flink job
STREAM_ASSET_SPECS: list[tuple[str, str, str | None]] = [
    ("stream_web_events", "event_id", "event_date"),
    ("stream_orders", "order_id", "order_date"),
    ("stream_order_items", "order_item_id", None),
    ("stream_payments", "payment_id", "payment_date"),
]


#: table name -> asset definition, for wiring downstream `deps=[...]` (lineage)
BRONZE_ASSETS_BY_NAME = {a.key.path[-1]: a for a in bronze_assets}

# The virtual stream assets join the layer (before the gate, so the gate
# waits for them too) and silver's fct_* merges depend on them for lineage.
for _name, _pk, _date_col in STREAM_ASSET_SPECS:
    _stream_a = _stream_asset(_name, _pk, _date_col)
    bronze_assets.append(_stream_a)
    BRONZE_ASSETS_BY_NAME[_name] = _stream_a

# Barrier asset for cross-table checks (referential integrity between
# bronze tables runs only after every bronze table is committed).
_bronze_gate = build_layer_gate("bronze", BRONZE_ASSETS_BY_NAME)
BRONZE_ASSETS_BY_NAME[GATE_NAME] = _bronze_gate
bronze_assets.append(_bronze_gate)
