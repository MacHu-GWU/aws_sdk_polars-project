# -*- coding: utf-8 -*-

import random

import pytest
import polars as pl
from s3pathlib import S3Path
from polars_writer.api import Writer

from aws_sdk_polars.constants import (
    S3_METADATA_KEY_N_RECORD,
    S3_METADATA_KEY_N_COLUMN,
)
from aws_sdk_polars.s3.write import (
    configure_s3_write_options,
    configure_s3path,
    partition_df_for_s3,
)


def test_configure_s3_write_options():
    df = pl.DataFrame({"id": [1, 2, 3]})

    writer = Writer(format="csv")
    s3_kwargs = {}
    ext = configure_s3_write_options(
        df=df,
        polars_writer=writer,
        gzip_compress=False,
        s3pathlib_write_bytes_kwargs=s3_kwargs,
    )
    assert ext == ".csv"
    assert s3_kwargs["content_type"] == "text/plain"
    assert s3_kwargs["metadata"][S3_METADATA_KEY_N_RECORD] == "3"
    assert s3_kwargs["metadata"][S3_METADATA_KEY_N_COLUMN] == "1"

    s3_kwargs = {}
    ext = configure_s3_write_options(
        df=df,
        polars_writer=writer,
        gzip_compress=True,
        s3pathlib_write_bytes_kwargs=s3_kwargs,
    )
    assert ext == ".csv.gzip"
    assert s3_kwargs["content_type"] == "text/plain"
    assert s3_kwargs["content_encoding"] == "gzip"

    writer = Writer(format="json")
    s3_kwargs = {}
    ext = configure_s3_write_options(
        df=df,
        polars_writer=writer,
        gzip_compress=False,
        s3pathlib_write_bytes_kwargs=s3_kwargs,
    )
    assert ext == ".json"
    assert s3_kwargs["content_type"] == "application/json"

    s3_kwargs = {}
    ext = configure_s3_write_options(
        df=df,
        polars_writer=writer,
        gzip_compress=True,
        s3pathlib_write_bytes_kwargs=s3_kwargs,
    )
    assert ext == ".json.gzip"
    assert s3_kwargs["content_type"] == "application/json"
    assert s3_kwargs["content_encoding"] == "gzip"

    writer = Writer(format="parquet")
    s3_kwargs = {}
    ext = configure_s3_write_options(
        df=df,
        polars_writer=writer,
        gzip_compress=False,
        s3pathlib_write_bytes_kwargs=s3_kwargs,
    )
    assert ext == ".snappy.parquet"
    assert s3_kwargs["content_type"] == "application/x-parquet"
    assert s3_kwargs["content_encoding"] == "snappy"

    writer = Writer(format="parquet")
    s3_kwargs = {"metadata": {"create_by": "polars utils"}}
    ext = configure_s3_write_options(
        df=df,
        polars_writer=writer,
        gzip_compress=True,
        s3pathlib_write_bytes_kwargs=s3_kwargs,
    )
    assert ext == ".gzip.parquet"
    assert s3_kwargs["metadata"]["create_by"] == "polars utils"
    assert s3_kwargs["metadata"][S3_METADATA_KEY_N_RECORD] == "3"
    assert s3_kwargs["metadata"][S3_METADATA_KEY_N_COLUMN] == "1"
    assert s3_kwargs["content_type"] == "application/x-parquet"
    assert s3_kwargs["content_encoding"] == "gzip"

    writer = Writer(format="parquet", parquet_compression="snappy")
    s3_kwargs = {}
    with pytest.raises(ValueError):
        ext = configure_s3_write_options(
            df=df,
            polars_writer=writer,
            gzip_compress=True,
            s3pathlib_write_bytes_kwargs=s3_kwargs,
        )

    writer = Writer(format="parquet", parquet_compression="snappy")
    s3_kwargs = {}
    ext = configure_s3_write_options(
        df=df,
        polars_writer=writer,
        gzip_compress=False,
        s3pathlib_write_bytes_kwargs=s3_kwargs,
    )
    assert ext == ".snappy.parquet"
    assert s3_kwargs["content_type"] == "application/x-parquet"
    assert s3_kwargs["content_encoding"] == "snappy"


def test_configure_s3path():
    s3dir = S3Path("s3://bucket/key/")
    fname = "data"
    ext = ".csv"
    s3path = S3Path("s3://bucket/key/data.tsv")

    s3path_new = configure_s3path(s3dir=s3dir, fname=fname, ext=ext)
    assert s3path_new.uri == "s3://bucket/key/data.csv"

    s3path_new = configure_s3path(s3path=s3path)
    assert s3path_new.uri == "s3://bucket/key/data.tsv"

    with pytest.raises(ValueError):
        s3path_new = configure_s3path()


def test_partition_df_for_s3():
    n_tag = 5
    tags = [f"tag-{i}" for i in range(1, 1 + n_tag)]
    n_row = 1000
    df = pl.DataFrame(
        {
            "id": range(1, 1 + n_row),
            "tag": [random.choice(tags) for _ in range(n_row)],
        }
    )
    s3dir = S3Path(f"s3://bucket/table/")
    results = list(
        partition_df_for_s3(
            df=df,
            s3dir=s3dir,
            part_keys=["tag"],
        )
    )
    assert len(results) == n_tag
    assert sum([df.shape[0] for df, _ in results]) == n_row


if __name__ == "__main__":
    from aws_sdk_polars.tests import run_cov_test

    run_cov_test(
        __file__,
        "aws_sdk_polars.s3.write",
        preview=False,
    )
