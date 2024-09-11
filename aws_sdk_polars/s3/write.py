# -*- coding: utf-8 -*-

"""
todo: docstring
"""

import typing as T

import polars as pl
from s3pathlib import S3Path
from polars_writer.writer import Writer

from ..constants import S3_METADATA_KEY_N_RECORD, S3_METADATA_KEY_N_COLUMN

from .partition import encode_hive_partition


def configure_s3_write_options(
    df: pl.DataFrame,
    polars_writer: Writer,
    gzip_compress: bool,
    s3pathlib_write_bytes_kwargs: T.Dict[str, T.Any],
) -> str:
    """
    Configure S3 write options based on the polars writer.

    This function sets up the necessary metadata and content-related parameters for
    writing a Polars DataFrame to S3. It determines the appropriate file extension
    and configures compression settings based on the writer format and user preferences.

    :param df: The Polars DataFrame to be written.
    :param polars_writer: The Polars writer object specifying the output format.
    :param gzip_compress: Whether to apply gzip compression (where applicable).
    :param s3pathlib_write_bytes_kwargs: Dictionary of keyword arguments
        for S3 write operation, to be modified in-place.

    :return: The appropriate file extension for the configured write operation.
    """
    more_metadata = {
        S3_METADATA_KEY_N_RECORD: str(df.shape[0]),
        S3_METADATA_KEY_N_COLUMN: str(df.shape[1]),
    }
    if "metadata" in s3pathlib_write_bytes_kwargs:
        s3pathlib_write_bytes_kwargs["metadata"].update(more_metadata)
    else:
        s3pathlib_write_bytes_kwargs["metadata"] = more_metadata

    if polars_writer.is_csv():
        s3pathlib_write_bytes_kwargs["content_type"] = "text/plain"
        if gzip_compress:
            s3pathlib_write_bytes_kwargs["content_encoding"] = "gzip"
            return ".csv.gzip"
        else:
            return ".csv"
    elif polars_writer.is_json() or polars_writer.is_ndjson():
        s3pathlib_write_bytes_kwargs["content_type"] = "application/json"
        if gzip_compress:
            s3pathlib_write_bytes_kwargs["content_encoding"] = "gzip"
            return ".json.gzip"
        else:
            return ".json"
    elif polars_writer.is_parquet():
        s3pathlib_write_bytes_kwargs["content_type"] = "application/x-parquet"
        if isinstance(polars_writer.parquet_compression, str):
            if gzip_compress is True:
                raise ValueError(
                    "For Parquet, gzip_compress must be False. "
                    "You should use Writer.parquet_compression to specify the compression."
                )
            s3pathlib_write_bytes_kwargs["content_encoding"] = (
                polars_writer.parquet_compression
            )
            return f".{polars_writer.parquet_compression}.parquet"
        else:
            # use snappy as the default compression
            polars_writer.parquet_compression = "snappy"
            if gzip_compress is True:
                polars_writer.parquet_compression = "gzip"
                s3pathlib_write_bytes_kwargs["content_encoding"] = "gzip"
                return ".gzip.parquet"
            else:
                s3pathlib_write_bytes_kwargs["content_encoding"] = "snappy"
                return ".snappy.parquet"
    elif polars_writer.is_delta():  # pragma: no cover
        raise NotImplementedError
    else:  # pragma: no cover
        raise ValueError(f"Unsupported format: {polars_writer.format}")


def configure_s3path(
    s3dir: T.Optional[S3Path] = None,
    fname: T.Optional[str] = None,
    ext: T.Optional[str] = None,
    s3path: T.Optional[S3Path] = None,
):
    """
    Configure and return an S3Path object for file operations.

    This function allows flexible specification of an S3 path. It can either construct
    a path from individual components (directory, filename, and extension) or use a
    pre-configured S3Path object.

    :param s3dir: The S3 directory path. Required if s3path is not provided.
    :param fname: The filename without extension. Required if s3path is not provided.
        for example, if the full file name is "data.csv", then fname is "data".
    :param ext: The file extension, including the dot (e.g., '.csv').
        Required if s3path is not provided.
    :param s3path: A pre-configured S3Path object. If provided, other arguments are ignored.

    :return The configured S3Path object representing the full file path in S3.
    """
    if s3path is None:
        if (s3dir is None) or (fname is None) or (ext is None):
            raise ValueError(
                "s3dir, fname, and ext must be provided when s3path is not provided"
            )
        return s3dir.joinpath(fname + ext)
    else:
        return s3path


def partition_df_for_s3(
    df: pl.DataFrame,
    s3dir: S3Path,
    part_keys: T.List[str],
) -> T.Iterator[T.Tuple[pl.DataFrame, S3Path]]:
    """
    Group dataframe by partition keys and locate the S3 location for each partition.

    :param df: ``polars.DataFrame`` object.
    :param s3dir: ``s3pathlib.S3Path`` object, the root directory of the
        S3 location of the datalake table.
    :param part_keys: list of partition keys. for example: ["year", "month"].
    """
    part_values: T.List[str]
    for part_values, sub_df in df.group_by(part_keys):
        sub_df = sub_df.drop(part_keys)
        kvs = dict(zip(part_keys, part_values))
        partition_relpath = encode_hive_partition(kvs=kvs)
        s3dir = s3dir.joinpath(partition_relpath).to_dir()
        yield (sub_df, s3dir)
