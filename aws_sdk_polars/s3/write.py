# -*- coding: utf-8 -*-

"""
todo: docstring
"""

import typing as T
import io

import polars as pl
from s3pathlib import S3Path
from polars_writer.writer import Writer, ParquetCompressionEnum
from compress.api import Algorithm, compress as do_compress

from ..constants import (
    S3_METADATA_KEY_N_RECORD,
    S3_METADATA_KEY_N_COLUMN,
)
from ..typehint import T_OPTIONAL_KWARGS

from .partition import encode_hive_partition

if T.TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_s3.client import S3Client


_content_encoding_mapping = {
    Algorithm.gzip.value: "gzip",
    Algorithm.bz2.value: "bz2",
    Algorithm.snappy.value: "snappy",
    Algorithm.lz4.value: "lz4",
    Algorithm.lzma.value: "lzo",
    Algorithm.zstd.value: "zstd",
}
_file_ext_mapping = {
    Algorithm.gzip.value: ".gz",
    Algorithm.bz2.value: ".bz2",
    Algorithm.snappy.value: ".snappy",
    Algorithm.lz4.value: ".lz4",
    Algorithm.lzma.value: ".lzo",
    Algorithm.zstd.value: ".zst",
}


def configure_s3_write_options(
    df: pl.DataFrame,
    polars_writer: Writer,
    compress: Algorithm,
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
    compress_str: str = Algorithm.ensure_str(compress)
    content_encoding = _content_encoding_mapping.get(compress_str)
    if content_encoding is not None:
        s3pathlib_write_bytes_kwargs["content_encoding"] = content_encoding
    compress_ext = _file_ext_mapping.get(compress_str, "")

    if polars_writer.is_csv():
        s3pathlib_write_bytes_kwargs["content_type"] = "text/plain"
        return f".csv{compress_ext}"
    elif polars_writer.is_json() or polars_writer.is_ndjson():
        s3pathlib_write_bytes_kwargs["content_type"] = "application/json"
        return f".json{compress_ext}"
    elif polars_writer.is_parquet():
        s3pathlib_write_bytes_kwargs["content_type"] = "application/x-parquet"
        compression = polars_writer.parquet_compression
        if isinstance(compression, str):
            if compression == ParquetCompressionEnum.uncompressed:
                return ".parquet"
            else:
                s3pathlib_write_bytes_kwargs["content_encoding"] = compression
                return f".{compression}.parquet"
        else:
            return ".parquet"
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
        s3dir_partition = s3dir.joinpath(partition_relpath).to_dir()
        yield (sub_df, s3dir_partition)


def write(
    df: pl.DataFrame,
    s3_client: "S3Client",
    polars_writer: Writer,
    compression: T.Union[str, Algorithm] = Algorithm.uncompressed,
    compression_kwargs: T_OPTIONAL_KWARGS = None,
    s3pathlib_write_bytes_kwargs: T_OPTIONAL_KWARGS = None,
    s3dir: T.Optional[S3Path] = None,
    fname: T.Optional[str] = None,
    s3path: T.Optional[S3Path] = None,
) -> S3Path:
    """
    Write the DataFrame to the given S3Path object, also attach
    additional information related to the dataframe.

    The original ``polars.write_parquet`` method doesn't work with moto,
    so we use buffer to store the parquet file and then write it to S3.

    :param df: ``polars.DataFrame`` object.
    :param s3_client: ``boto3.client("s3")`` object.
    :param polars_writer: `polars_writer.api.Writer <https://github.com/MacHu-GWU/polars_writer-project>`_
        object.
    :param compression: compression method for CSV, JSON. This option is ignored
        for parquet, deltalake formats. Because it is already defined in polars_writer.
    :param compression_kwargs: compression method keyword arguments.
        For example, for `gzip <https://docs.python.org/3/library/gzip.html>`,
        you can provide `{"compresslevel": 9}`.
    :param s3pathlib_write_bytes_kwargs: Keyword arguments for
        ``s3path.write_bytes`` method. See
        https://s3pathlib.readthedocs.io/en/latest/s3pathlib/core/rw.html#s3pathlib.core.rw.ReadAndWriteAPIMixin.write_bytes
    :param s3dir: The S3 directory path. Required if s3path is not provided.
    :param fname: The filename without extension. Required if s3path is not provided.
        for example, if the full file name is "data.csv", then fname is "data".
    :param s3path: A pre-configured S3Path object. If provided, other arguments are ignored.

    :return: the S3Path object representing the created file on S3. You could
        access its attribute like 'size', 'etag', 'last_modified_at'
    """
    # --- preprocess input arguments
    if s3pathlib_write_bytes_kwargs is None:
        s3pathlib_write_bytes_kwargs = {}

    ext = configure_s3_write_options(
        df=df,
        polars_writer=polars_writer,
        compress=compression,
        s3pathlib_write_bytes_kwargs=s3pathlib_write_bytes_kwargs,
    )
    if (
        polars_writer.is_csv()
        or polars_writer.is_json()
        or polars_writer.is_ndjson()
        or polars_writer.is_parquet()
    ):
        buffer = io.BytesIO()
        polars_writer.write(df, file_args=[buffer])
        b = buffer.getvalue()
        b = do_compress(algo=compression, data=b, kwargs=compression_kwargs)
        s3path = configure_s3path(
            s3dir=s3dir,
            fname=fname,
            ext=ext,
            s3path=s3path,
        )
        s3path_new = s3path.write_bytes(
            b,
            bsm=s3_client,
            **s3pathlib_write_bytes_kwargs,
        )
        return s3path_new
    elif polars_writer.is_delta():  # pragma: no cover
        # if s3dir is None:
        #     raise ValueError("s3dir must be provided for deltalake formats")
        # polars_writer.write(df, file_args=[s3dir.uri])
        # return s3dir
        raise NotImplementedError
    else:  # pragma: no cover
        raise NotImplementedError
