"""Structured Streaming sink for file-collector S3 metadata events.

The incoming DataFrame is the raw Kafka source DataFrame. Its ``value`` JSON
must follow the file-collector schema, including ``eventId``, ``fileName``,
``fileType``, ``bucket``, and ``objectKey``. Each referenced CSV has columns:

    BodyLength, BodyPixel, Body, TailLength, TailPixel, Tail

Body rows are assigned negative order values (-N through -1), and Tail rows
are assigned positive order values (1 through M). The rows are written
directly from each Spark partition to Oracle, so the parsed CSV data does not
need to be converted back into a Spark DataFrame.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from decimal import Decimal
from io import BytesIO
from typing import Any, Iterator
from urllib.parse import unquote, urlparse

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import LongType, StringType, StructField, StructType


MEASUREMENT_COLUMNS = [
    "BodyLength",
    "BodyPixel",
    "Body",
    "TailLength",
    "TailPixel",
    "Tail",
]

FILE_METADATA_SCHEMA = StructType(
    [
        StructField("schemaVersion", LongType(), False),
        StructField("eventId", StringType(), False),
        StructField("deviceName", StringType(), False),
        StructField("fileName", StringType(), False),
        StructField("fileType", StringType(), False),
        StructField("fileSize", LongType(), False),
        StructField("checksumAlgorithm", StringType(), False),
        StructField("checksum", StringType(), False),
        StructField("bucket", StringType(), False),
        StructField("objectKey", StringType(), False),
        StructField("eTag", StringType(), True),
        StructField("versionId", StringType(), True),
        StructField("uploadedAt", StringType(), False),
    ]
)


@dataclass(frozen=True)
class CsvOracleSinkConfig:
    checkpoint_location: str
    query_name: str = "s3-csv-oracle-writer"
    kafka_value_column: str = "value"


def _validated_oracle_identifier(value: str) -> str:
    """Allow only ordinary Oracle schema/table identifiers."""
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_$#]*(\.[A-Za-z][A-Za-z0-9_$#]*)?", value):
        raise ValueError(f"Invalid Oracle table identifier: {value!r}")
    return value


def _oracle_merge_sql(table_name: str) -> str:
    table_name = _validated_oracle_identifier(table_name)
    return f"""
        MERGE INTO {table_name} T
        USING (
            SELECT
                :1 AS LOT_ID,
                :2 AS MS_CODE,
                :3 AS CLASS_TYPE,
                :4 AS LENGTH_VALUE,
                :5 AS PIXEL_VALUE,
                :6 AS DIA_VALUE,
                :7 AS ROW_ORDER_VALUE
            FROM DUAL
        ) S
        ON (
            T.LOT_ID = S.LOT_ID
            AND T.MS_CODE = S.MS_CODE
            AND T.CLASS_TYPE = S.CLASS_TYPE
            AND T.ROW_ORDER = S.ROW_ORDER_VALUE
        )
        WHEN MATCHED THEN
            UPDATE SET
                T.LENGTH = S.LENGTH_VALUE,
                T.PIXEL = S.PIXEL_VALUE,
                T.DIA = S.DIA_VALUE
        WHEN NOT MATCHED THEN
            INSERT (
                LOT_ID,
                MS_CODE,
                CLASS_TYPE,
                LENGTH,
                PIXEL,
                DIA,
                ROW_ORDER
            )
            VALUES (
                S.LOT_ID,
                S.MS_CODE,
                S.CLASS_TYPE,
                S.LENGTH_VALUE,
                S.PIXEL_VALUE,
                S.DIA_VALUE,
                S.ROW_ORDER_VALUE
            )
    """


def _parse_s3_uri(path: str) -> tuple[str, str]:
    parsed = urlparse(path)
    if parsed.scheme not in {"s3", "s3a"} or not parsed.netloc:
        raise ValueError(f"Expected an s3:// or s3a:// URI, got: {path!r}")
    return parsed.netloc, unquote(parsed.path.lstrip("/"))


def _lot_and_ms_code(file_name: str) -> tuple[str, str]:
    stem = file_name.rsplit(".", 1)[0]
    parts = stem.split("_")
    if len(parts) < 4:
        raise ValueError(
            "CSV file name must contain LOT_ID and MS_CODE as its third and "
            f"fourth underscore-separated values: {file_name!r}"
        )
    return parts[2], parts[3]


def _read_s3_csv(
    s3_client: Any,
    path: str,
    version_id: str | None = None,
) -> Any:
    # Imports are local because this function runs inside a Python executor.
    import pandas as pd

    bucket, key = _parse_s3_uri(path)
    request = {"Bucket": bucket, "Key": key}
    if version_id:
        request["VersionId"] = version_id
    response = s3_client.get_object(**request)
    try:
        content = response["Body"].read()
    finally:
        response["Body"].close()

    pdf = pd.read_csv(
        BytesIO(content),
        dtype="string",
        usecols=MEASUREMENT_COLUMNS,
    )
    return pdf.replace(r"^\s*$", pd.NA, regex=True)


def _measurement_rows(pdf: Any, file_name: str) -> list[tuple[Any, ...]]:
    lot_id, ms_code = _lot_and_ms_code(file_name)
    rows: list[tuple[Any, ...]] = []

    body_pdf = pdf.dropna(
        subset=["BodyLength", "BodyPixel", "Body"]
    ).reset_index(drop=True)
    body_count = len(body_pdf)

    for position, row in enumerate(body_pdf.itertuples(index=False), start=1):
        rows.append(
            (
                lot_id,
                ms_code,
                "Body",
                Decimal(str(row.BodyLength)),
                int(row.BodyPixel),
                Decimal(str(row.Body)),
                position - body_count - 1,
            )
        )

    tail_pdf = pdf.dropna(
        subset=["TailLength", "TailPixel", "Tail"]
    ).reset_index(drop=True)

    for position, row in enumerate(tail_pdf.itertuples(index=False), start=1):
        rows.append(
            (
                lot_id,
                ms_code,
                "Tail",
                Decimal(str(row.TailLength)),
                int(row.TailPixel),
                Decimal(str(row.Tail)),
                position,
            )
        )

    return rows


def process_csv_partition(path_rows: Iterator[Any]) -> None:
    """Parse all files in one non-empty Spark partition using one DB session.

    Required executor environment variables:
      ORACLE_USER, ORACLE_PASSWORD, ORACLE_DSN, ORACLE_TARGET_TABLE

    AWS credentials must also be available to boto3 on every executor.
    Custom S3 installations can set S3_ENDPOINT_URL, AWS_REGION,
    S3_PATH_STYLE, and S3_VERIFY_TLS on the executors.
    """
    import boto3
    import oracledb
    from botocore.config import Config

    connection = None
    cursor = None
    s3_client = None

    try:
        for path_row in path_rows:
            # Connect lazily so an empty partition creates no Oracle session.
            if connection is None:
                table_name = os.environ["ORACLE_TARGET_TABLE"]
                connection = oracledb.connect(
                    user=os.environ["ORACLE_USER"],
                    password=os.environ["ORACLE_PASSWORD"],
                    dsn=os.environ["ORACLE_DSN"],
                )
                cursor = connection.cursor()
                verify_tls = os.getenv("S3_VERIFY_TLS", "true").lower() not in {
                    "0",
                    "false",
                    "no",
                }
                addressing_style = (
                    "path"
                    if os.getenv("S3_PATH_STYLE", "false").lower()
                    in {"1", "true", "yes"}
                    else "auto"
                )
                s3_client = boto3.client(
                    "s3",
                    endpoint_url=os.getenv("S3_ENDPOINT_URL") or None,
                    region_name=(
                        os.getenv("AWS_REGION")
                        or os.getenv("AWS_DEFAULT_REGION")
                        or None
                    ),
                    verify=verify_tls,
                    config=Config(s3={"addressing_style": addressing_style}),
                )
                merge_sql = _oracle_merge_sql(table_name)

            path = path_row.path
            file_name = path_row.file_name
            version_id = path_row.version_id
            pdf = _read_s3_csv(s3_client, path, version_id)
            oracle_rows = _measurement_rows(pdf, file_name)

            if oracle_rows:
                cursor.executemany(merge_sql, oracle_rows)

            # One CSV file is one transaction. MERGE keeps task retries
            # idempotent for the configured business key.
            connection.commit()

    except Exception:
        if connection is not None:
            connection.rollback()
        raise
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()


def parse_file_metadata_stream(
    kafka_stream_df: DataFrame,
    *,
    value_column: str = "value",
) -> DataFrame:
    """Parse raw Kafka values using the file-collector metadata schema."""
    parsed_df = kafka_stream_df.withColumn(
        "_metadata",
        F.from_json(F.col(value_column).cast("string"), FILE_METADATA_SCHEMA),
    )

    return (
        parsed_df.filter(F.col("_metadata").isNotNull())
        .select(
            F.col("_metadata.*"),
            *[
                F.col(name)
                for name in ("topic", "partition", "offset", "timestamp")
                if name in kafka_stream_df.columns
            ],
        )
        .filter(F.col("schemaVersion") == 1)
        .filter(F.upper(F.col("fileType")) == "CSV")
        .filter(
            F.col("eventId").isNotNull()
            & F.col("fileName").isNotNull()
            & F.col("bucket").isNotNull()
            & F.col("objectKey").isNotNull()
        )
    )


def process_csv_batch(batch_df: DataFrame, batch_id: int) -> None:
    """Process one Kafka microbatch without collecting file paths to driver."""
    del batch_id

    paths_df = (
        batch_df.select(
            "eventId",
            F.concat(
                F.lit("s3://"),
                F.col("bucket"),
                F.lit("/"),
                F.regexp_replace(F.col("objectKey"), r"^/", ""),
            ).alias("path"),
            F.col("fileName").alias("file_name"),
            F.col("versionId").alias("version_id"),
        )
        # eventId is deterministic for device + bucket + key + checksum.
        .dropDuplicates(["eventId"])
        # The workload is intentionally small. This limits the sink to one
        # non-empty task and therefore at most one Oracle session per batch.
        .coalesce(1)
    )

    paths_df.select("path", "file_name", "version_id").foreachPartition(
        process_csv_partition
    )


def start_csv_oracle_query(
    kafka_stream_df: DataFrame,
    config: CsvOracleSinkConfig,
) -> StreamingQuery:
    """Start the CSV-to-Oracle streaming query and return its handle."""

    csv_metadata_stream_df = parse_file_metadata_stream(
        kafka_stream_df,
        value_column=config.kafka_value_column,
    )

    def foreach_batch(batch_df: DataFrame, batch_id: int) -> None:
        process_csv_batch(batch_df, batch_id)

    return (
        csv_metadata_stream_df.writeStream.foreachBatch(foreach_batch)
        .option("checkpointLocation", config.checkpoint_location)
        .queryName(config.query_name)
        .start()
    )
