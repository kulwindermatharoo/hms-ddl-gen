from dataclasses import dataclass
from typing import List


@dataclass
class TableMetadata:
    database_name: str
    table_name: str
    tbl_id: int
    owner: str
    tbl_type: str
    sd_id: int


@dataclass
class ColumnMetadata:
    column_name: str
    type_name: str
    comment: str


@dataclass
class TableStorageInfo:
    input_format: str
    output_format: str
    location: str
    serde_id: int
    cd_id: int


@dataclass
class SerdeMetadata:
    slib: str
    name: str


@dataclass
class SerdeParam:
    param_key: str
    param_value: str


@dataclass
class TableParam:
    param_key: str
    param_value: str


@dataclass
class PartitionKeyMetadata:
    pkey_name: str
    pkey_type: str
    pkey_comment: str
