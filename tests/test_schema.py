# -*- coding: utf-8 -*-

import typing as T
import pytest

import polars as pl

from aws_sdk_polars.schema import (
    merge_schemas,
)


def make_schema(record: T.Dict[str, T.Any]) -> pl.Schema:
    return pl.DataFrame([record]).schema


ms = make_schema


class TestMergeSchemas:
    def case1(self):
        s1 = ms({"a": 1, "b": 1})
        s2 = ms({"a": 1})
        s3 = ms({"a": 1, "b": 1, "c": 1})
        s = ms({"a": 1, "b": 1, "c": 1})
        s_out = merge_schemas([s1, s2, s3])
        # print(s_out)  # for debug only
        assert s_out == s

    def case2(self):
        s1 = ms({"a": 1, "b": 1.23})
        s2 = ms({"b": "abc", "c": 1})
        with pytest.raises(TypeError) as e:
            merge_schemas([s1, s2])

    def case3(self):
        s1 = ms({"a_struct": {"a": 1, "b": 2}})
        s2 = ms({"a_struct": {"b": 2, "c": 3}})
        s = ms({"a_struct": {"a": 1, "b": 2, "c": 3}})

        s_out = merge_schemas([s1, s2])
        print(s_out)  # for debug only
        assert s_out == s

    def test(self):
        # self.case1()
        # self.case2()
        self.case3()


if __name__ == "__main__":
    from aws_sdk_polars.tests import run_cov_test

    run_cov_test(__file__, "aws_sdk_polars.schema", preview=False)
