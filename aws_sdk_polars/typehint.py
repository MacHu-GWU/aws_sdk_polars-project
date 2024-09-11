# -*- coding: utf-8 -*-

try:
    import typing_extensions as T
except:  # pragma: no cover
    import typing as T

T_RECORD = T.Dict[str, T.Any]
T_OPTIONAL_KWARGS = T.Optional[T.Dict[str, T.Any]]


class StorageOptions(T.TypedDict):
    aws_region: T.Required[str]
    aws_access_key_id: T.Required[str]
    aws_secret_access_key: T.Required[str]
    aws_session_token: T.NotRequired[str]
