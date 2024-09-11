# -*- coding: utf-8 -*-

import pytest

import polars as pl
from s3pathlib import S3Path
from polars_writer.api import Writer

from aws_sdk_polars.s3.read import (
    read_csv,
    read_json,
    read_ndjson,
    read_parquet,
    read_many_csv,
    read_many_json,
    read_many_ndjson,
    read_many_parquet,
)
from aws_sdk_polars.s3.write import write
from aws_sdk_polars.tests.mock_aws import BaseMockAwsTest


class Test(BaseMockAwsTest):
    use_mock: bool = True

    def test_read(self):
        df1 = pl.DataFrame({"id": [1, 2], "name": ["alice", "bob"]})
        df2 = pl.DataFrame({"id": [3, 4], "name": ["cathy", "david"]})
        df3 = pl.DataFrame(
            {"id": [5, 6], "name": ["edward", "frank"], "gender": [1, 0]}
        )
        df4 = pl.DataFrame({"id": [7, 8]})

        s3path1 = S3Path(f"s3://{self.bucket}/1.parquet")
        s3path2 = S3Path(f"s3://{self.bucket}/2.parquet")
        s3path3 = S3Path(f"s3://{self.bucket}/3.parquet")
        s3path4 = S3Path(f"s3://{self.bucket}/4.parquet")

        csv_writer = Writer(format="csv")
        json_writer = Writer(format="json")
        ndjson_writer = Writer(format="ndjson")
        parquet_writer = Writer(format="parquet", parquet_compression="gzip")

        case_list = [
            (csv_writer, read_csv, read_many_csv),
            (json_writer, read_json, read_many_json),
            (ndjson_writer, read_ndjson, read_many_ndjson),
            (parquet_writer, read_parquet, read_many_parquet),
        ]
        for writer, read, read_many in case_list:
            for df, s3path in [
                (df1, s3path1),
                (df2, s3path2),
                (df3, s3path3),
                (df4, s3path4),
            ]:
                s3path_new = write(
                    df=df,
                    s3_client=self.s3_client,
                    polars_writer=writer,
                    compression="gzip",
                    s3path=s3path,
                )

            df = read(s3path=s3path1, s3_client=self.s3_client, decompress="gzip")
            assert df.to_dicts() == [
                {"id": 1, "name": "alice"},
                {"id": 2, "name": "bob"},
            ]

            df = read_many(
                s3path_list=[s3path1, s3path2],
                s3_client=self.s3_client,
                decompress="gzip",
            )
            assert df.to_dicts() == [
                {"id": 1, "name": "alice"},
                {"id": 2, "name": "bob"},
                {"id": 3, "name": "cathy"},
                {"id": 4, "name": "david"},
            ]

            with pytest.raises(Exception):
                df = read_many(
                    s3path_list=[s3path1, s3path2, s3path3, s3path4],
                    s3_client=self.s3_client,
                    decompress="gzip",
                )

            df = read_many(
                s3path_list=[s3path1, s3path2, s3path3, s3path4],
                s3_client=self.s3_client,
                decompress="gzip",
                merge_col=True,
            )
            assert df.to_dicts() == [
                {"id": 1, "name": "alice", "gender": None},
                {"id": 2, "name": "bob", "gender": None},
                {"id": 3, "name": "cathy", "gender": None},
                {"id": 4, "name": "david", "gender": None},
                {"id": 5, "name": "edward", "gender": 1},
                {"id": 6, "name": "frank", "gender": 0},
                {"id": 7, "name": None, "gender": None},
                {"id": 8, "name": None, "gender": None},
            ]


if __name__ == "__main__":
    from aws_sdk_polars.tests import run_cov_test

    run_cov_test(
        __file__,
        "aws_sdk_polars.s3.read",
        preview=False,
    )
