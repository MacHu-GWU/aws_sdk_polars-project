# -*- coding: utf-8 -*-

from aws_sdk_polars import api


def test():
    _ = api
    _ = api.s3.partition
    _ = api.s3.partition.encode_hive_partition
    _ = api.s3.partition.decode_hive_partition
    _ = api.s3.partition.build_hive_partition_dir
    _ = api.s3.partition.S3Partition
    _ = api.s3.partition.list_partitions
    _ = api.s3.partition_df_for_s3


if __name__ == "__main__":
    from aws_sdk_polars.tests import run_cov_test

    run_cov_test(__file__, "aws_sdk_polars.api", preview=False)