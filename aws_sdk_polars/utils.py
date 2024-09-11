# -*- coding: utf-8 -*-

import typing as T
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


def get_merged_schema(
    dfs: T.List[pl.DataFrame],
    raise_on_conflict: bool = True,
) -> T.Dict[str, pl.DataType]:
    """
    Merge the schemas of multiple Polars DataFrames into a single schema.

    This function takes a list of Polars DataFrames and combines their schemas.
    If multiple DataFrames have columns with the same name but different types,
    the type from the last DataFrame in the list takes precedence.

    :param dfs: A list of Polars DataFrames whose schemas are to be merged.

    :return: A dictionary representing the merged schema. Keys are column names,
        and values are Polars DataTypes.

    **Example**

    >>> import polars as pl
    >>> df1 = pl.DataFrame({'A': [1, 2], 'B': ['a', 'b']})
    >>> df2 = pl.DataFrame({'B': [1.0, 2.0], 'C': [True, False]})
    >>> merged_schema = get_merged_schema([df1, df2])
    >>> print(merged_schema)
    {'A': Int64, 'B': Float64, 'C': Boolean}

    .. note::

        This function does not handle conflicts between data types. If the same
        column name appears with different types in multiple DataFrames, the type
        from the last DataFrame in the list will be used in the merged schema.
    """
    merged_schema = dict()
    for df in dfs:
        for col_name, data_type in df.schema.items():
            if col_name in merged_schema:
                if merged_schema[col_name] != data_type:
                    if raise_on_conflict:
                        raise TypeError(
                            f"Schema conflict detected for column '{col_name}': "
                            f"{merged_schema[col_name]} vs {data_type}"
                        )
            merged_schema[col_name] = data_type
    return merged_schema


def harmonize_schemas(
    dfs: T.List[pl.DataFrame],
    schema: T.Dict[str, pl.DataType],
) -> T.List[pl.DataFrame]:
    """
    Harmonize the schemas of multiple Polars DataFrames to match a given schema.

    This function takes a list of DataFrames and a target schema, then modifies
    each DataFrame to conform to this schema. It adds missing columns with NULL
    values and the specified data type from the schema. Existing columns in the
    DataFrames are left unchanged, even if their data type differs from the schema.

    :param dfs: A list of Polars DataFrames whose schemas are to be harmonized.
    :param schema: A dictionary representing the target schema. Keys are column names,
        and values are Polars DataTypes.

    :return: A new list of DataFrames with harmonized schemas. Each DataFrame in this
        list corresponds to a DataFrame in the input list, but with added columns
        to match the target schema.

    **Example**

    >>> import polars as pl
    >>> df1 = pl.DataFrame({'A': [1, 2], 'B': ['a', 'b']})
    >>> df2 = pl.DataFrame({'B': [1.0, 2.0], 'C': [True, False]})
    >>> target_schema = {'A': pl.Int64, 'B': pl.Utf8, 'C': pl.Boolean, 'D': pl.Float64}
    >>> harmonized_dfs = harmonize_schemas([df1, df2], target_schema)
    >>> print(harmonized_dfs[0].schema)
    {'A': Int64, 'B': Utf8, 'C': Boolean, 'D': Float64}
    >>> print(harmonized_dfs[1].schema)
    {'B': Float64, 'C': Boolean, 'A': Int64, 'D': Float64}

    .. note::

        1. This function only adds missing columns; it does not modify or remove
           existing columns in the input DataFrames.
        2. The data type of existing columns is not changed, even if it differs
           from the type specified in the target schema.
        3. Added columns are filled with NULL values of the appropriate type.
    """
    new_dfs = list()
    for df in dfs:
        this_schema = set(df.schema)
        merged_schema = dict(schema)
        for k in this_schema:
            merged_schema.pop(k)
        new_columns = [pl.lit(None, dtype=v).alias(k) for k, v in merged_schema.items()]
        df = df.with_columns(*new_columns)
        new_dfs.append(df)
    return new_dfs


def merge_dataframes(
    dfs: T.List[pl.DataFrame],
) -> pl.DataFrame:
    """
    Merge multiple Polars DataFrames into a single DataFrame with a unified schema.

    This function performs the following steps:

    1. Merges the schemas of all input DataFrames, raising an error if there are conflicts.
    2. Harmonizes the schemas of all DataFrames to match the merged schema.
    3. Concatenates all harmonized DataFrames into a single DataFrame.

    :param dfs: A list of Polars DataFrames to be merged.

    :return: A single Polars DataFrame containing all data from the input DataFrames,
        with a schema that includes all columns from all input DataFrames.

    :raises ValueError: If there are schema conflicts between the input DataFrames
        (e.g., same column name with different data types).

    **Example**

    >>> import polars as pl
    >>> df1 = pl.DataFrame({'A': [1, 2], 'B': ['a', 'b']})
    >>> df2 = pl.DataFrame({'B': ['c', 'd'], 'C': [True, False]})
    >>> merged_df = merge_dataframes([df1, df2])
    >>> print(merged_df)
    shape: (4, 3)
    ┌─────┬─────┬───────┐
    │ A   ┆ B   ┆ C     │
    │ --- ┆ --- ┆ ---   │
    │ i64 ┆ str ┆ bool  │
    ╞═════╪═════╪═══════╡
    │ 1   ┆ a   ┆ null  │
    │ 2   ┆ b   ┆ null  │
    │ null┆ c   ┆ true  │
    │ null┆ d   ┆ false │
    └─────┴─────┴───────┘

    .. note::

        - This function uses :func:`get_merged_schema` with ``raise_on_conflict=True``,
          so it will raise an error if there are any schema conflicts.
        - The :func:`harmonize_schemas` function is used to ensure all DataFrames
          have the same schema before concatenation.
        - The order of rows in the output DataFrame corresponds to the order
          of DataFrames in the input list.
    """
    schema = get_merged_schema(dfs, raise_on_conflict=True)
    dfs_harmonized = harmonize_schemas(dfs, schema)
    cols = list(schema)
    return pl.concat([df.select(cols) for df in dfs_harmonized])
