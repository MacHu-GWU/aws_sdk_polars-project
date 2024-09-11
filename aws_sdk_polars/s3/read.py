# -*- coding: utf-8 -*-

"""
todo: docstring
"""

import typing as T

import polars as pl
from s3pathlib import S3Path
from compress.api import Algorithm, decompress as do_decompress

from ..typehint import T_OPTIONAL_KWARGS
from ..utils import merge_dataframes

if T.TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_s3.client import S3Client


def _read(
    s3path: S3Path,
    s3_client: "S3Client",
    pl_meth: str,
    pl_kwargs: T_OPTIONAL_KWARGS = None,
    decompress: T.Union[str, Algorithm] = Algorithm.uncompressed,
    decompress_kwargs: T_OPTIONAL_KWARGS = None,
) -> pl.DataFrame:
    if pl_kwargs is None:
        pl_kwargs = {}
    b = s3path.read_bytes(bsm=s3_client)
    b = do_decompress(algo=decompress, data=b, kwargs=decompress_kwargs)
    df = getattr(pl, pl_meth)(b, **pl_kwargs)
    return df


def _read_many(
    s3path_list: T.Iterable[S3Path],
    s3_client: "S3Client",
    pl_meth: str,
    pl_kwargs: T_OPTIONAL_KWARGS = None,
    decompress: T.Union[str, Algorithm] = Algorithm.uncompressed,
    decompress_kwargs: T_OPTIONAL_KWARGS = None,
    merge_col: bool = False,
) -> pl.DataFrame:
    sub_df_list = list()
    for s3path in s3path_list:
        sub_df = _read(
            s3path=s3path,
            s3_client=s3_client,
            pl_meth=pl_meth,
            pl_kwargs=pl_kwargs,
            decompress=decompress,
            decompress_kwargs=decompress_kwargs,
        )
        sub_df_list.append(sub_df)
    if merge_col:
        df = merge_dataframes(dfs=sub_df_list)
    else:
        df = pl.concat(sub_df_list)
    return df


def read_csv(
    s3path: S3Path,
    s3_client: "S3Client",
    pl_kwargs: T_OPTIONAL_KWARGS = None,
    decompress: T.Union[str, Algorithm] = Algorithm.uncompressed,
    decompress_kwargs: T_OPTIONAL_KWARGS = None,
) -> pl.DataFrame:
    return _read(
        s3path=s3path,
        s3_client=s3_client,
        pl_meth="read_csv",
        pl_kwargs=pl_kwargs,
        decompress=decompress,
        decompress_kwargs=decompress_kwargs,
    )


def read_json(
    s3path: S3Path,
    s3_client: "S3Client",
    pl_kwargs: T_OPTIONAL_KWARGS = None,
    decompress: T.Union[str, Algorithm] = Algorithm.uncompressed,
    decompress_kwargs: T_OPTIONAL_KWARGS = None,
) -> pl.DataFrame:
    return _read(
        s3path=s3path,
        s3_client=s3_client,
        pl_meth="read_json",
        pl_kwargs=pl_kwargs,
        decompress=decompress,
        decompress_kwargs=decompress_kwargs,
    )


def read_ndjson(
    s3path: S3Path,
    s3_client: "S3Client",
    pl_kwargs: T_OPTIONAL_KWARGS = None,
    decompress: T.Union[str, Algorithm] = Algorithm.uncompressed,
    decompress_kwargs: T_OPTIONAL_KWARGS = None,
) -> pl.DataFrame:
    return _read(
        s3path=s3path,
        s3_client=s3_client,
        pl_meth="read_ndjson",
        pl_kwargs=pl_kwargs,
        decompress=decompress,
        decompress_kwargs=decompress_kwargs,
    )


def read_parquet(
    s3path: S3Path,
    s3_client: "S3Client",
    pl_kwargs: T_OPTIONAL_KWARGS = None,
    decompress: T.Union[str, Algorithm] = Algorithm.uncompressed,
    decompress_kwargs: T_OPTIONAL_KWARGS = None,
) -> pl.DataFrame:
    return _read(
        s3path=s3path,
        s3_client=s3_client,
        pl_meth="read_parquet",
        pl_kwargs=pl_kwargs,
        decompress=decompress,
        decompress_kwargs=decompress_kwargs,
    )


def read_many_csv(
    s3path_list: T.Iterable[S3Path],
    s3_client: "S3Client",
    pl_kwargs: T_OPTIONAL_KWARGS = None,
    decompress: T.Union[str, Algorithm] = Algorithm.uncompressed,
    decompress_kwargs: T_OPTIONAL_KWARGS = None,
    merge_col: bool = False,
) -> pl.DataFrame:
    return _read_many(
        s3path_list=s3path_list,
        s3_client=s3_client,
        pl_meth="read_csv",
        pl_kwargs=pl_kwargs,
        decompress=decompress,
        decompress_kwargs=decompress_kwargs,
        merge_col=merge_col,
    )


def read_many_json(
    s3path_list: T.Iterable[S3Path],
    s3_client: "S3Client",
    pl_kwargs: T_OPTIONAL_KWARGS = None,
    decompress: T.Union[str, Algorithm] = Algorithm.uncompressed,
    decompress_kwargs: T_OPTIONAL_KWARGS = None,
    merge_col: bool = False,
) -> pl.DataFrame:
    return _read_many(
        s3path_list=s3path_list,
        s3_client=s3_client,
        pl_meth="read_json",
        pl_kwargs=pl_kwargs,
        decompress=decompress,
        decompress_kwargs=decompress_kwargs,
        merge_col=merge_col,
    )


def read_many_ndjson(
    s3path_list: T.Iterable[S3Path],
    s3_client: "S3Client",
    pl_kwargs: T_OPTIONAL_KWARGS = None,
    decompress: T.Union[str, Algorithm] = Algorithm.uncompressed,
    decompress_kwargs: T_OPTIONAL_KWARGS = None,
    merge_col: bool = False,
) -> pl.DataFrame:
    return _read_many(
        s3path_list=s3path_list,
        s3_client=s3_client,
        pl_meth="read_ndjson",
        pl_kwargs=pl_kwargs,
        decompress=decompress,
        decompress_kwargs=decompress_kwargs,
        merge_col=merge_col,
    )


def read_many_parquet(
    s3path_list: T.Iterable[S3Path],
    s3_client: "S3Client",
    pl_kwargs: T_OPTIONAL_KWARGS = None,
    decompress: T.Union[str, Algorithm] = Algorithm.uncompressed,
    decompress_kwargs: T_OPTIONAL_KWARGS = None,
    merge_col: bool = False,
) -> pl.DataFrame:
    return _read_many(
        s3path_list=s3path_list,
        s3_client=s3_client,
        pl_meth="read_parquet",
        pl_kwargs=pl_kwargs,
        decompress=decompress,
        decompress_kwargs=decompress_kwargs,
        merge_col=merge_col,
    )
