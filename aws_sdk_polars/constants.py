# -*- coding: utf-8 -*-

from .vendor.better_enum import BetterStrEnum


# S3 Object Metadata Keys
S3_METADATA_KEY_SIZE = "size"
S3_METADATA_KEY_N_RECORD = "n_record"
S3_METADATA_KEY_N_COLUMN = "n_column"


# ------------------------------------------------------------------------------
# Enum
# ------------------------------------------------------------------------------
class CompressionEnum(BetterStrEnum):
    """
    Enumeration of supported compression algorithms for Parquet files.
    """

    uncompressed = "uncompressed"
    gzip = "gzip"
    bzip2 = "bzip2"
    snappy = "snappy"
    zstd = "zstd"
    lz4 = "lz4"
    lzo = "lzo"
    brotli = "brotli"
