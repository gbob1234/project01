"""Structured Streaming sink for S3 measurement CSV files.

The incoming streaming DataFrame is expected to contain ``directory`` and
``filename`` columns. Each referenced CSV file has these columns:

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


MEASUREMENT_COLUMNS = [
    "BodyLength",
    "BodyPixel",
    "Body",
    "TailLength",
    "TailPixel",
    "Tail",
]


@dataclass(frozen=True)
class CsvOracleSinkConfig:
    checkpoint_location: str
    query_name: str = "s3-csv-oracle-writer"
    directory_column: str = "directory"
    filename_column: str = "filename"
    path_column: str = "path"


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


def _file_name_from_uri(path: str) -> str:
    _, key = _parse_s3_uri(path)
    file_name = key.rsplit("/", 1)[-1]
    if not file_name:
        raise ValueError(f"S3 URI does not contain a file name: {path!r}")
    return file_name


def _lot_and_ms_code(file_name: str) -> tuple[str, str]:
    stem = file_name.rsplit(".", 1)[0]
    parts = stem.split("_")
    if len(parts) < 4:
        raise ValueError(
            "CSV file name must contain LOT_ID and MS_CODE as its third and "
            f"fourth underscore-separated values: {file_name!r}"
        )
    return parts[2], parts[3]


def _read_s3_csv(s3_client: Any, path: str) -> Any:
    # Imports are local because this function runs inside a Python executor.
    import pandas as pd

    bucket, key = _parse_s3_uri(path)
    response = s3_client.get_object(Bucket=bucket, Key=key)
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
    """
    import boto3
    import oracledb

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
                s3_client = boto3.client("s3")
                merge_sql = _oracle_merge_sql(table_name)

            path = path_row.path
            file_name = _file_name_from_uri(path)
            pdf = _read_s3_csv(s3_client, path)
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


def process_csv_batch(
    batch_df: DataFrame,
    batch_id: int,
    *,
    directory_column: str = "directory",
    filename_column: str = "filename",
    path_column: str = "path",
) -> None:
    """Process one Kafka microbatch without collecting file paths to driver."""
    del batch_id

    paths_df = (
        batch_df.select(
            F.concat_ws(
                "/",
                F.regexp_replace(F.col(directory_column), r"/$", ""),
                F.regexp_replace(F.col(filename_column), r"^/", ""),
            ).alias(path_column)
        )
        .filter(F.col(path_column).isNotNull())
        .dropDuplicates([path_column])
        # The workload is intentionally small. This limits the sink to one
        # non-empty task and therefore at most one Oracle session per batch.
        .coalesce(1)
    )

    # process_csv_partition expects a Row attribute named "path".
    normalized_paths_df = paths_df.select(F.col(path_column).alias("path"))
    normalized_paths_df.foreachPartition(process_csv_partition)


def start_csv_oracle_query(
    csv_stream_df: DataFrame,
    config: CsvOracleSinkConfig,
) -> StreamingQuery:
    """Start the CSV-to-Oracle streaming query and return its handle."""

    def foreach_batch(batch_df: DataFrame, batch_id: int) -> None:
        process_csv_batch(
            batch_df,
            batch_id,
            directory_column=config.directory_column,
            filename_column=config.filename_column,
            path_column=config.path_column,
        )

    return (
        csv_stream_df.writeStream.foreachBatch(foreach_batch)
        .option("checkpointLocation", config.checkpoint_location)
        .queryName(config.query_name)
        .start()
    )
