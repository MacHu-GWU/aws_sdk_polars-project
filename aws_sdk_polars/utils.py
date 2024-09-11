# -*- coding: utf-8 -*-

import polars as pl
from tabulate import tabulate


def df_to_ascii(df: pl.DataFrame) -> str:
    """
    Convert a polars DataFrame to an ASCII table in string.
    """
    return tabulate(
        [list(record.values()) for record in df.to_dicts()],
        headers=list(df.schema),
        tablefmt="grid",
    )


def pprint_df(df: pl.DataFrame):  # pragma: no cover
    """
    Pretty print a polars DataFrame.
    """
    print(df_to_ascii(df))
