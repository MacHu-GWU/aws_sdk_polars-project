# -*- coding: utf-8 -*-

import typing as T
import copy
import polars as pl

T_SCHEMA = T.Dict[str, pl.DataType]


def merge_schemas(
    schemas: T.List[pl.Schema],
    _is_struct: bool = False,
) -> pl.Schema:
    merged_schema = dict()
    for schema in schemas:
        for col_name, source_dtype in schema.items():
            if col_name in merged_schema:
                target_dtype = merged_schema[col_name]
                print(f"{col_name = }, {source_dtype = }, {target_dtype = }")
                if isinstance(source_dtype, pl.Struct) and isinstance(target_dtype, pl.Struct):
                    merged_struct = merge_schemas(
                        schemas=[
                            {field.name: field.dtype for field in source_dtype.fields},
                            {field.name: field.dtype for field in target_dtype.fields},
                        ],
                        _is_struct=True,
                    )
                    merged_schema[col_name] = merged_struct
                elif target_dtype != source_dtype:
                    raise TypeError(
                        f"Schema conflict detected for column '{col_name}': "
                        f"{target_dtype} vs {source_dtype}"
                    )
            else:
                merged_schema[col_name] = source_dtype
    if _is_struct:
        return pl.Struct(merged_schema)
    else:
        return pl.Schema(merged_schema)


def adapt_schema(
    source_schema: T.Dict[str, pl.DataType],
    target_schema: T.Dict[str, pl.DataType],
) -> T.List[pl.DataFrame]:
    """ """
    source_schema = copy.deepcopy(source_schema)
    for k, source_dtype in target_schema.items():
        if k in target_schema:
            target_dtype = target_schema[k]
            if isinstance(target_dtype, pl.Struct):
                raise
            elif isinstance(target_dtype, pl.List):
                raise
            else:
                pass
        else:
            source_schema[k] = target_schema[k]

    return source_schema
