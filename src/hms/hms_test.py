from data_model import PartitionKeyMetadata, TableParam, SerdeMetadata, TableStorageInfo, ColumnMetadata, \
    TableMetadata
from typing import List

from hms_show_create import generate_create_table_statement
from src.hms.common_utils import CommonUtils

if __name__ == '__main__':

    project_root = CommonUtils.find_project_root()
    print(project_root)

    # Example usage:
    table_metadata = TableMetadata(
        database_name="my_database",
        table_name="my_table",
        tbl_id=12345,
        owner="admin",
        tbl_type="managed",
        sd_id=676
    )

    column_metadata = [
        ColumnMetadata("id", "int", "Primary Key"),
        ColumnMetadata("name", "string", "User's name")
    ]

    partition_keys = [
        PartitionKeyMetadata("year", "int", "Partition by year"),
        PartitionKeyMetadata("month", "int", "Partition by month")
    ]

    storage_info = TableStorageInfo(
        input_format="org.apache.hadoop.mapred.TextInputFormat",
        output_format="org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
        location="s3://my-bucket/my-table/",
        serde_id=12345
    )

    serde_metadata = SerdeMetadata(
        lib="org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
        name="Text SerDe"
    )

    table_params = [
        TableParam("partitioned_by", "year, month"),
        TableParam("compression", "snappy")
    ]

    # Generate the CREATE TABLE statement
    create_table_sql = generate_create_table_statement(
        table_metadata, column_metadata, partition_keys, storage_info, serde_metadata, table_params
    )

    print(create_table_sql)

    print("test")
