# -*- coding: utf-8 -*-

from textwrap import dedent

import polars as pl

from aws_sdk_polars.utils import df_to_ascii


def test_df_to_ascii():
    df = pl.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
    s = df_to_ascii(df)
    s1 = dedent(
        """
    +------+--------+
    |   id | name   |
    +======+========+
    |    1 | Alice  |
    +------+--------+
    |    2 | Bob    |
    +------+--------+
    """
    ).strip()
    assert s == s1


if __name__ == "__main__":
    from aws_sdk_polars.tests import run_cov_test

    run_cov_test(__file__, "aws_sdk_polars.utils", preview=False)
