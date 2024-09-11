# -*- coding: utf-8 -*-

if __name__ == "__main__":
    from aws_sdk_polars.tests import run_cov_test

    run_cov_test(
        __file__,
        "aws_sdk_polars.s3",
        is_folder=True,
        preview=False,
    )
