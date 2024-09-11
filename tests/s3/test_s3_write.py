# -*- coding: utf-8 -*-

import pytest
import gzip
import random

import polars as pl
from s3pathlib import S3Path
from polars_writer.api import Writer
from compress.api import Algorithm

from aws_sdk_polars.constants import (
    S3_METADATA_KEY_N_RECORD,
    S3_METADATA_KEY_N_COLUMN,
)
from aws_sdk_polars.s3.write import (
    configure_s3_write_options,
    configure_s3path,
    partition_df_for_s3,
    write,
)
from aws_sdk_polars.tests.mock_aws import BaseMockAwsTest


def test_configure_s3_write_options():
    df = pl.DataFrame({"id": [1, 2, 3]})

    writer = Writer(format="csv")
    s3_kwargs = {"metadata": {"create_by": "alice"}}
    ext = configure_s3_write_options(
        df=df,
        polars_writer=writer,
        compress=Algorithm.uncompressed,
        s3pathlib_write_bytes_kwargs=s3_kwargs,
    )
    assert ext == ".csv"
    assert s3_kwargs["content_type"] == "text/plain"
    assert s3_kwargs["metadata"]["create_by"] == "alice"
    assert s3_kwargs["metadata"][S3_METADATA_KEY_N_RECORD] == "3"
    assert s3_kwargs["metadata"][S3_METADATA_KEY_N_COLUMN] == "1"

    s3_kwargs = {}
    ext = configure_s3_write_options(
        df=df,
        polars_writer=writer,
        compress=Algorithm.gzip,
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
        compress=Algorithm.uncompressed,
        s3pathlib_write_bytes_kwargs=s3_kwargs,
    )
    assert ext == ".json"
    assert s3_kwargs["content_type"] == "application/json"

    s3_kwargs = {}
    ext = configure_s3_write_options(
        df=df,
        polars_writer=writer,
        compress=Algorithm.gzip,
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
        compress=Algorithm.uncompressed,
        s3pathlib_write_bytes_kwargs=s3_kwargs,
    )
    assert ext == ".parquet"
    assert s3_kwargs["content_type"] == "application/x-parquet"

    writer = Writer(format="parquet", parquet_compression="uncompressed")
    s3_kwargs = {}
    ext = configure_s3_write_options(
        df=df,
        polars_writer=writer,
        compress=Algorithm.uncompressed,
        s3pathlib_write_bytes_kwargs=s3_kwargs,
    )
    assert ext == ".parquet"
    assert s3_kwargs["content_type"] == "application/x-parquet"

    writer = Writer(format="parquet", parquet_compression="gzip")
    s3_kwargs = {}
    ext = configure_s3_write_options(
        df=df,
        polars_writer=writer,
        compress=Algorithm.uncompressed,
        s3pathlib_write_bytes_kwargs=s3_kwargs,
    )
    assert ext == ".gzip.parquet"
    assert s3_kwargs["content_type"] == "application/x-parquet"
    assert s3_kwargs["content_encoding"] == "gzip"


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


class Test(BaseMockAwsTest):
    use_mock: bool = True

    def test_write(self):
        # case 1
        df = pl.DataFrame({"id": [1, 2, 3], "name": ["alice", "bob", "cathy"]})
        s3path = S3Path(f"s3://{self.bucket}/1.csv")
        s3path_new = write(
            df=df,
            s3_client=self.s3_client,
            polars_writer=Writer(
                format="csv",
            ),
            compression="uncompressed",
            s3path=s3path,
        )
        assert isinstance(s3path_new.size, int)
        assert isinstance(s3path_new.etag, str)

        text = s3path_new.read_text(bsm=self.s3_client)
        assert text.splitlines() == [
            "id,name",
            "1,alice",
            "2,bob",
            "3,cathy",
        ]
        assert s3path_new.metadata == {
            "n_record": "3",
            "n_column": "2",
        }
        assert s3path_new.response["ContentType"] == "text/plain"

        # case 2
        s3dir = S3Path(f"s3://{self.bucket}/")
        fname = "1"
        s3path_new = write(
            df=df,
            s3_client=self.s3_client,
            polars_writer=Writer(
                format="ndjson",
            ),
            compression=Algorithm.gzip,
            s3dir=s3dir,
            fname=fname,
        )
        text = gzip.decompress(s3path_new.read_bytes(bsm=self.s3_client)).decode(
            "utf-8"
        )
        assert text.splitlines() == [
            '{"id":1,"name":"alice"}',
            '{"id":2,"name":"bob"}',
            '{"id":3,"name":"cathy"}',
        ]
        assert s3path_new.metadata == {
            "n_record": "3",
            "n_column": "2",
        }
        assert s3path_new.response["ContentType"] == "application/json"
        assert s3path_new.response["ContentEncoding"] == "gzip"


if __name__ == "__main__":
    from aws_sdk_polars.tests import run_cov_test

    run_cov_test(
        __file__,
        "aws_sdk_polars.s3.write",
        preview=False,
    )
