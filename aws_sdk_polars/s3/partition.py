# -*- coding: utf-8 -*-

"""
Datalake partition utilities.

This module provides functions and classes for managing and manipulating
partitions in a data lake stored on S3. It includes utilities for extracting
partition information, encoding partition data, and listing partitions.
"""

import typing as T
import dataclasses

from s3pathlib import S3Path

if T.TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_s3.client import S3Client


def decode_hive_partition(
    s3dir_root: S3Path,
    s3dir_partition: S3Path,
) -> T.Dict[str, str]:
    """
    Extract partition data from the S3 directory path.

    :param s3dir_root: The root S3 directory.
    :param s3dir_partition: The partition S3 directory.

    **Example**

    >>> s3dir_root = S3Path("s3://bucket/folder/")
    >>> s3dir_partition = S3Path("s3://bucket/folder/year=2021/month=01/day=15/")
    >>> decode_hive_partition(s3dir_root, s3dir_partition)
    {"year": "2021", "month": "01", "day": "15"}
    """
    data = dict()
    for part in s3dir_partition.relative_to(s3dir_root).parts:
        key, value = part.split("=", 1)
        data[key] = value
    return data


def encode_hive_partition(kvs: T.Dict[str, str]) -> str:
    """
    Encode partition data into hive styled partition string.

    :param kvs: A dictionary of partition key-value pairs.

    For example:

        >>> encode_hive_partition({"year": "2021", "month": "01", "day": "01"})
        'year=2021/month=01/day=01'
    """
    return "/".join([f"{k}={v}" for k, v in kvs.items()])


def build_hive_partition_dir(
    s3dir_root: S3Path,
    kvs: T.Dict[str, str],
) -> S3Path:
    """
    Get the S3 directory path of the partition.

    :param s3dir_root: The root S3 directory.
    :param kvs: A dictionary of partition key-value pairs.

    **Example**

    >>> s3dir_root = S3Path("s3://bucket/folder/")
    >>> s3dir_partition = build_hive_partition_dir(s3dir_root, {"year": "2021", "month": "01", "day": "01"})
    >>> s3dir_partition.uri
    's3://bucket/folder/year=2021/month=01/day=01/'
    """
    return (s3dir_root / encode_hive_partition(kvs)).to_dir()


@dataclasses.dataclass
class S3Partition:
    """
    Represents a partition in an S3-based data lake.

    A partition is a directory in S3 that contains data files but no subdirectories.
    It typically follows a hierarchical structure based on partition keys.

    For example, in the following S3 directory structure::

        s3://bucket/folder/year=2021/month=01/day=01/data.json
        s3://bucket/folder/year=2021/month=01/day=02/data.json
        s3://bucket/folder/year=2021/month=02/day=01/data.json
        s3://bucket/folder/year=2021/month=02/day=02/data.json

    Then:

    - ``s3://bucket/folder/year=2021/month=01/day=01/`` is a partition.
    - ``s3://bucket/folder/year=2021/month=01/`` is NOT a partition.
    - ``s3://bucket/folder/year=2021/`` is NOT a partition.

    :param root_uri: The S3 URI of the root folder of partition. For example:
        The root folder of ``s3://bucket/folder/year=2021/month=01/day=01/``
        is ``s3://bucket/folder/``.
    :param data: A dictionary of partition data. Note that the value is always
        a string, even if it represents a number.
        For example: ``{"year": "2021", "month": "01", "day": "01"}``
    """

    root_uri: str = dataclasses.field()
    data: T.Dict[str, str] = dataclasses.field()

    def __post_init__(self):
        if self.root_uri.endswith("/") is False:
            self.root_uri = self.root_uri + "/"

    @property
    def s3dir_root(self) -> S3Path:
        """
        The S3 directory path of the root directory.

        If the partition is "s3://bucket/folder/year=2021/month=01/day=15/",
        then the root directory is "s3://bucket/folder/".
        """
        return S3Path.from_s3_uri(self.root_uri)

    @property
    def s3dir_part(self) -> S3Path:
        """
        The S3 directory path of the partition.
        """
        return build_hive_partition_dir(self.s3dir_root, self.data)

    @property
    def part_uri(self) -> str:
        """
        The S3 URI of the partition directory.
        """
        return self.s3dir_part.uri

    @classmethod
    def from_uri(
        cls,
        s3uri_part: str,
        s3uri_root: str,
    ):
        """
        Construct a Partition object from S3 URIs.

        :param s3dir_part: The S3 URI of the partition.
        :param s3uri_root: The S3 URI of the root directory.

        :return: A new :class:`Partition` object.
        """
        s3dir_part = S3Path.from_s3_uri(s3uri_part).to_dir()
        s3dir_root = S3Path.from_s3_uri(s3uri_root).to_dir()
        data = decode_hive_partition(s3dir_root, s3dir_part)
        return cls(root_uri=s3uri_root, data=data)

    @classmethod
    def from_part_uri(
        cls,
        part_uri: str,
        n_levels: int,
    ):
        """
        Construct a Partition object from a partition URI.

        :param part_uri: The S3 URI of the partition.
        :param n_levels: The number of levels to go up to reach the root directory.

        :return: A new :class:`Partition` object.
        """
        s3dir_part = S3Path.from_s3_uri(part_uri).to_dir()
        s3dir_root = s3dir_part
        for _ in range(n_levels):
            s3dir_root = s3dir_root.parent
        return cls.from_uri(s3uri_part=part_uri, s3uri_root=s3dir_root.uri)

    def list_files_by_ext(
        self,
        s3_client: "S3Client",
        ext: str,
    ) -> T.List[S3Path]:
        """
        List files in the partition by file extension. Files in subdirectories
        are not included.

        :param ext: File extension to filter. For example, ".parquet".

        :return: A list of S3Path objects representing Parquet files.
        """
        return (
            self.s3dir_part.iterdir(bsm=s3_client)
            .filter(lambda x: x.basename.endswith(ext))
            .all()
        )


def list_partitions(
    s3_client: "S3Client",
    s3dir_root: S3Path,
) -> T.List[S3Partition]:
    """
    Efficiently scan the S3 directory and return a list of partitions.

    For example, for the following S3 structure:

        s3://bucket/folder/year=2021/month=01/day=01/data.json
        s3://bucket/folder/year=2021/month=01/day=02/data.json
        s3://bucket/folder/year=2021/month=02/day=01/data.json
        s3://bucket/folder/year=2021/month=02/day=02/data.json

    The function will return partitions::

        s3://bucket/folder/year=2021/month=01/day=01/
        s3://bucket/folder/year=2021/month=01/day=02/
        s3://bucket/folder/year=2021/month=02/day=01/
        s3://bucket/folder/year=2021/month=02/day=02/

    .. note::

        This implementation has higher performance compared to
        :func:`get_partitions_v1` as it avoids recursive S3 API calls.
    """
    # locate all s3 folder that has file in it
    s3_uri_set = {
        s3path.parent.uri for s3path in s3dir_root.iter_objects(bsm=s3_client)
    }
    s3_uri_list = list()
    # make sure either it is the s3dir_root or it has "=" character in it
    len_s3dir_root = len(s3dir_root.uri)
    for s3_uri in s3_uri_set:
        # sometimes we may have non partition folder, such as ``.hoodie`` folder
        # so we should check if there's a "=" character in it.
        if ("=" in s3_uri.split("/")[-2]) or (len(s3_uri) == len_s3dir_root):
            s3_uri_list.append(s3_uri)
    # convert partition uri list to partition object list
    s3_uri_list.sort()
    partition_list = list()
    for s3_uri in s3_uri_list:
        s3dir = S3Path.from_s3_uri(s3_uri)
        data = decode_hive_partition(s3dir_root, s3dir)
        partition = S3Partition(root_uri=s3dir_root.uri, data=data)
        partition_list.append(partition)
    return partition_list
