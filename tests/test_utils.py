# -*- coding: utf-8 -*-

import pytest
from textwrap import dedent

import polars as pl

from aws_sdk_polars.utils import (
    df_to_ascii,
    get_merged_schema,
    harmonize_schemas,
    merge_dataframes,
)


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


def test_harmonize_schemas():
    df1 = pl.DataFrame(
        {
            "a": [1, 2, 3],
            "b": ["a", "b", "c"],
        },
    )
    df2 = pl.DataFrame(
        {
            "b": ["a", "b", "c"],
            "c": {
                "id": 1,
                "name": "Alice",
            },
        },
    )
    df3 = pl.DataFrame(
        {
            "b": [1.1, 2.2, 3.3],
        },
    )
    dfs = [df1, df2, df3]

    with pytest.raises(TypeError):
        get_merged_schema(dfs, raise_on_conflict=True)
    schema = get_merged_schema(dfs, raise_on_conflict=False)
    assert isinstance(schema["b"], pl.Float64)

    df1_res, df2_res = harmonize_schemas([df1, df2], schema)
    # print(df1_res, df2_res)  # for debug only
    assert set(df1_res.schema) == {"a", "b", "c"}
    assert set(df2_res.schema) == {"a", "b", "c"}

    with pytest.raises(TypeError):
        merge_dataframes([df1, df2, df3])

    df = merge_dataframes([df1, df2])
    # print(df)  # for debug only
    assert set(df.schema) == {"a", "b", "c"}
    assert df.shape == (6, 3)


if __name__ == "__main__":
    from aws_sdk_polars.tests import run_cov_test

    run_cov_test(__file__, "aws_sdk_polars.utils", preview=False)
