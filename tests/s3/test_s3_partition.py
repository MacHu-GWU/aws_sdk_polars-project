# -*- coding: utf-8 -*-

import pytest
from s3pathlib import S3Path

from aws_sdk_polars.s3.partition import (
    S3Partition,
    decode_hive_partition,
    encode_hive_partition,
    build_hive_partition_dir,
    list_partitions,
)
from aws_sdk_polars.tests.mock_aws import BaseMockAwsTest


def test_decode_hive_partition():
    data = decode_hive_partition(
        s3dir_partition=S3Path("s3://bucket/folder/year=2021/month=01/day=15/"),
        s3dir_root=S3Path("s3://bucket/folder/"),
    )
    assert data == {"year": "2021", "month": "01", "day": "15"}

    data = decode_hive_partition(
        s3dir_partition=S3Path("s3://bucket/folder/"),
        s3dir_root=S3Path("s3://bucket/folder/"),
    )
    assert data == {}

    with pytest.raises(Exception):
        data = decode_hive_partition(
            s3dir_partition=S3Path("s3://bucket/"),
            s3dir_root=S3Path("s3://bucket/folder/"),
        )


def test_encode_hive_partition():
    relpath = encode_hive_partition({"year": "2021", "month": "01", "day": "01"})
    assert relpath == "year=2021/month=01/day=01"

    relpath = encode_hive_partition({})
    assert relpath == ""


def test_build_hive_partition_dir():
    s3dir_root = S3Path("s3://bucket/data/")
    kvs = {"year": "2021", "month": "07", "day": "01"}
    uri = build_hive_partition_dir(s3dir_root=s3dir_root, kvs=kvs).uri
    assert uri == "s3://bucket/data/year=2021/month=07/day=01/"

    s3dir_root = S3Path("s3://bucket/data/")
    kvs = {}
    uri = build_hive_partition_dir(s3dir_root=s3dir_root, kvs=kvs).uri
    assert uri == "s3://bucket/data/"


class TestS3Partition(BaseMockAwsTest):
    use_mock: bool = True

    def test_from_uri(self):
        part = S3Partition.from_uri(
            s3uri_part="s3://bucket/data/year=01",
            s3uri_root="s3://bucket/data",
        )
        assert part.root_uri == "s3://bucket/data/"
        assert part.data == {"year": "01"}
        assert part.s3dir_root.uri == "s3://bucket/data/"
        assert part.part_uri == "s3://bucket/data/year=01/"
        assert part.s3dir_part.uri == "s3://bucket/data/year=01/"

        part = S3Partition.from_uri(
            s3uri_part="s3://bucket/data/",
            s3uri_root="s3://bucket/data/",
        )
        assert part.root_uri == "s3://bucket/data/"
        assert part.data == {}
        assert part.s3dir_root.uri == "s3://bucket/data/"
        assert part.part_uri == "s3://bucket/data/"
        assert part.s3dir_part.uri == "s3://bucket/data/"

    def test_from_part_uri(self):
        part = S3Partition.from_part_uri(
            part_uri="s3://bucket/data/year=01",
            n_levels=1,
        )
        assert part.root_uri == "s3://bucket/data/"
        assert part.data == {"year": "01"}
        assert part.s3dir_root.uri == "s3://bucket/data/"
        assert part.part_uri == "s3://bucket/data/year=01/"
        assert part.s3dir_part.uri == "s3://bucket/data/year=01/"

        part = S3Partition.from_part_uri(
            part_uri="s3://bucket/data/",
            n_levels=0,
        )
        assert part.root_uri == "s3://bucket/data/"
        assert part.data == {}
        assert part.s3dir_root.uri == "s3://bucket/data/"
        assert part.part_uri == "s3://bucket/data/"
        assert part.s3dir_part.uri == "s3://bucket/data/"

    def test_list_partitions(self):
        # --- case 1
        s3dir_root = S3Path(f"s3://{self.bucket}/root1/")
        (s3dir_root / "year=2021/month=01/1.json").write_text("", bsm=self.s3_client)
        (s3dir_root / "year=2021/month=02/1.json").write_text("", bsm=self.s3_client)
        (s3dir_root / "year=2021/month=03/1.json").write_text("", bsm=self.s3_client)
        (s3dir_root / "year=2022/month=01/1.json").write_text("", bsm=self.s3_client)
        (s3dir_root / "year=2022/month=02/1.json").write_text("", bsm=self.s3_client)
        (s3dir_root / "year=2022/month=03/1.json").write_text("", bsm=self.s3_client)
        partitions = list_partitions(
            s3_client=self.s3_client,
            s3dir_root=s3dir_root,
        )
        assert len(partitions) == 6

        part = partitions[0]
        s3path_list = part.list_files_by_ext(s3_client=self.s3_client, ext=".json")
        assert len(s3path_list) == 1
        assert (
            s3path_list[0].uri == f"s3://{self.bucket}/root1/year=2021/month=01/1.json"
        )


if __name__ == "__main__":
    from aws_sdk_polars.tests import run_cov_test

    run_cov_test(
        __file__,
        "aws_sdk_polars.s3.partition",
        preview=False,
    )
