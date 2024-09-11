# -*- coding: utf-8 -*-

"""
This module provides functions for reading data from S3 into Polars DataFrames.

It includes functions for reading various file formats (CSV, JSON, NDJSON, Parquet, ...)
from S3, with support for decompression and reading multiple files.
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
    """
    Internal function to read a single file from S3 into a Polars DataFrame.

    :param s3path: S3Path object representing the file to be read.
    :param s3_client: Boto3 S3 client.
    :param pl_meth: Name of the Polars method to use for reading (e.g., 'read_csv').
    :param pl_kwargs: Optional keyword arguments for the Polars read method.
    :param decompress: Decompression algorithm to use, if the file is compressed.
    :param decompress_kwargs: Optional keyword arguments for decompression.

    :return: Polars DataFrame containing the data from the S3 file.
    """
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
    """
    Internal function to read multiple files from S3 into a single Polars DataFrame.

    :param s3path_list: Iterable of S3Path objects representing the files to be read.
    :param s3_client: Boto3 S3 client.
    :param pl_meth: Name of the Polars method to use for reading (e.g., 'read_csv').
    :param pl_kwargs: Optional keyword arguments for the Polars read method.
    :param decompress: Decompression algorithm to use, if the files are compressed.
    :param decompress_kwargs: Optional keyword arguments for decompression.
    :param merge_col: If True, merge columns of different schemas;
        if False, use simple concatenation (raises error when schemas differ).

    :return: Polars DataFrame containing the combined data from all S3 files.
    """
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
    """
    Read a CSV file from S3 into a Polars DataFrame.

    :param s3path: S3Path object representing the CSV file to be read.
    :param s3_client: Boto3 S3 client.
    :param pl_kwargs: Optional keyword arguments for Polars read_csv method.
    :param decompress: Decompression algorithm to use, if the file is compressed.
    :param decompress_kwargs: Optional keyword arguments for decompression.

    :return: Polars DataFrame containing the data from the CSV file.
    """
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
    """
    Read a JSON file from S3 into a Polars DataFrame.

    :param s3path: S3Path object representing the JSON file to be read.
    :param s3_client: Boto3 S3 client.
    :param pl_kwargs: Optional keyword arguments for Polars read_json method.
    :param decompress: Decompression algorithm to use, if the file is compressed.
    :param decompress_kwargs: Optional keyword arguments for decompression.

    :return: Polars DataFrame containing the data from the JSON file.
    """
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
    """
    Read an NDJSON (Newline Delimited JSON) file from S3 into a Polars DataFrame.

    :param s3path: S3Path object representing the NDJSON file to be read.
    :param s3_client: Boto3 S3 client.
    :param pl_kwargs: Optional keyword arguments for Polars read_ndjson method.
    :param decompress: Decompression algorithm to use, if the file is compressed.
    :param decompress_kwargs: Optional keyword arguments for decompression.

    :return: Polars DataFrame containing the data from the NDJSON file.
    """
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
    """
    Read a Parquet file from S3 into a Polars DataFrame.

    :param s3path: S3Path object representing the Parquet file to be read.
    :param s3_client: Boto3 S3 client.
    :param pl_kwargs: Optional keyword arguments for Polars read_parquet method.
    :param decompress: Decompression algorithm to use, if the file is compressed.
    :param decompress_kwargs: Optional keyword arguments for decompression.

    :return: Polars DataFrame containing the data from the Parquet file.
    """
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
    """
    Read multiple CSV files from S3 into a single Polars DataFrame.

    :param s3path_list: Iterable of S3Path objects representing the CSV files to be read.
    :param s3_client: Boto3 S3 client.
    :param pl_kwargs: Optional keyword arguments for Polars read_csv method.
    :param decompress: Decompression algorithm to use, if the files are compressed.
    :param decompress_kwargs: Optional keyword arguments for decompression.
    :param merge_col: If True, merge columns of different schemas; if False, use simple concatenation.

    :return: Polars DataFrame containing the combined data from all CSV files.
    """
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
    """
    Read multiple JSON files from S3 into a single Polars DataFrame.

    :param s3path_list: Iterable of S3Path objects representing the JSON files to be read.
    :param s3_client: Boto3 S3 client.
    :param pl_kwargs: Optional keyword arguments for Polars read_json method.
    :param decompress: Decompression algorithm to use, if the files are compressed.
    :param decompress_kwargs: Optional keyword arguments for decompression.
    :param merge_col: If True, merge columns of different schemas; if False, use simple concatenation.

    :return: Polars DataFrame containing the combined data from all JSON files.
    """
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
    """
    Read multiple NDJSON files from S3 into a single Polars DataFrame.

    :param s3path_list: Iterable of S3Path objects representing the NDJSON files to be read.
    :param s3_client: Boto3 S3 client.
    :param pl_kwargs: Optional keyword arguments for Polars read_ndjson method.
    :param decompress: Decompression algorithm to use, if the files are compressed.
    :param decompress_kwargs: Optional keyword arguments for decompression.
    :param merge_col: If True, merge columns of different schemas; if False, use simple concatenation.

    :return: Polars DataFrame containing the combined data from all NDJSON files.
    """
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
    """
    Read multiple Parquet files from S3 into a single Polars DataFrame.

    :param s3path_list: Iterable of S3Path objects representing the Parquet files to be read.
    :param s3_client: Boto3 S3 client.
    :param pl_kwargs: Optional keyword arguments for Polars read_parquet method.
    :param decompress: Decompression algorithm to use, if the files are compressed.
    :param decompress_kwargs: Optional keyword arguments for decompression.
    :param merge_col: If True, merge columns of different schemas; if False, use simple concatenation.

    :return: Polars DataFrame containing the combined data from all Parquet files.
    """
    return _read_many(
        s3path_list=s3path_list,
        s3_client=s3_client,
        pl_meth="read_parquet",
        pl_kwargs=pl_kwargs,
        decompress=decompress,
        decompress_kwargs=decompress_kwargs,
        merge_col=merge_col,
    )
