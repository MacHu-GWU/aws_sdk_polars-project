# -*- coding: utf-8 -*-

"""
boto3 related utilities.
"""

import boto3
from .typehint import StorageOptions


def get_storage_options(boto_ses: boto3.Session) -> StorageOptions:
    """
    Get the ``storage_options`` parameter for
    ``polars.read_xyz`` and ``polars.scan_xyz`` functions.
    """
    cred = boto_ses.get_credentials()
    dct = {
        "aws_region": boto_ses.region_name,
        "aws_access_key_id": cred.access_key,
        "aws_secret_access_key": cred.secret_key,
    }
    if cred.token:
        dct["aws_session_token"] = cred.token
    return dct
