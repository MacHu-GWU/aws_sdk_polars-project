# -*- coding: utf-8 -*-

import typing as T
import moto
import boto3
from s3pathlib import S3Path, context
from boto_session_manager import BotoSesManager

if T.TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_s3.client import S3Client
    from mypy_boto3_dynamodb.client import DynamoDBClient
    from mypy_boto3_glue.client import GlueClient
    from mypy_boto3_redshift.client import RedshiftClient
    from mypy_boto3_opensearch.client import OpenSearchServiceClient


class BaseMockAwsTest:
    use_mock: bool = True

    mock_aws: "moto.mock_aws" = None
    bucket: str = None
    s3dir_root: S3Path = None
    bsm: BotoSesManager = None
    boto_ses: boto3.Session = None
    s3_client: "S3Client" = None
    dynamodb_client: "DynamoDBClient" = None
    glue_client: "GlueClient" = None
    redshift_client: "RedshiftClient" = None
    oss_client: "OpenSearchServiceClient" = None

    @classmethod
    def setup_class(cls):
        if cls.use_mock:
            cls.mock_aws = moto.mock_aws()
            cls.mock_aws.start()

        if cls.use_mock:
            cls.bsm = BotoSesManager(region_name="us-east-1")
        else:
            cls.bsm = BotoSesManager(
                profile_name="bmt_app_dev_us_east_1",
                region_name="us-east-1",
            )

        cls.bucket = f"{cls.bsm.aws_account_id}-us-east-1-data"
        cls.s3dir_root = S3Path(f"s3://{cls.bucket}/projects/aws_sdk_polars/tests/")
        cls.boto_ses = cls.bsm.boto_ses
        context.attach_boto_session(cls.boto_ses)

        cls.s3_client = cls.bsm.s3_client
        cls.dynamodb_client = cls.bsm.dynamodb_client
        cls.glue_client = cls.bsm.glue_client
        cls.redshift_client = cls.bsm.redshift_client
        cls.oss_client = cls.bsm.opensearchservice_client

        cls.s3_client.create_bucket(Bucket=cls.bucket)

        cls.setup_class_post_hook()

    @classmethod
    def setup_class_post_hook(cls):
        pass

    @classmethod
    def teardown_class(cls):
        if cls.use_mock:
            cls.mock_aws.stop()
