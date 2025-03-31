from pyspark.sql import SparkSession
from dataclasses import make_dataclass, dataclass
from typing import List
from pyspark.sql.functions import expr

from data_model import PartitionKeyMetadata, TableParam, SerdeMetadata, TableStorageInfo, ColumnMetadata, \
    TableMetadata, SerdeParam


class HmsUtils:
    @staticmethod
    def get_table_metadata(spark: SparkSession, db_name: str, tb_name: str) -> TableMetadata:
        """

        Returns:
            object: 
        """
        query = f"""
            SELECT d.name AS database_name, t.tbl_name AS table_name, t.tbl_id, t.owner, t.tbl_type, t.sd_id
            FROM dbs d
            JOIN tbls t ON d.db_id = t.db_id
            WHERE d.name = '{db_name}' AND t.tbl_name = '{tb_name}';
        """

        df = spark.sql(query)

        # Collect results
        row = df.collect()

        if not row:
            return None  # Return None if no data found

        # Extract the first row
        row = row[0]

        # Create an instance of the TableMetadata dataclass with the retrieved values
        return TableMetadata(
            database_name=row["database_name"],
            table_name=row["table_name"],
            tbl_id=row["tbl_id"],
            owner=row["owner"],
            tbl_type=row["tbl_type"],
            sd_id=row["sd_id"]
        )

    @staticmethod
    def get_table_columns(spark: SparkSession, cd_id: int) -> List[ColumnMetadata]:

        query = f"""
                SELECT c.column_name, c.type_name, c.comment
                FROM columns_v2 c
                
                WHERE c.cd_id = {cd_id}
                order by cast(c.INTEGER_IDX as int)
                ;
            """

        # query = f"""
        #     SELECT c.column_name, c.type_name, c.comment
        #     FROM columns_v2 c
        #     JOIN sds s ON c.cd_id = s.cd_id
        #     WHERE s.sd_id = (
        #       SELECT sd_id FROM tbls WHERE tbl_id = {tbl_id}
        #     );
        # """

        df = spark.sql(query)

        # Collect results
        rows = df.collect()

        if not rows:
            return []  # Return an empty list if no columns found

        # Convert rows into a list of ColumnMetadata objects
        return [ColumnMetadata(
            column_name=row["column_name"],
            type_name=row["type_name"],
            comment=row["comment"] or ""  # Handle NULL comments
        ) for row in rows]

    @staticmethod
    def get_table_storage_info(spark: SparkSession, s_id: int) -> TableStorageInfo:
        query = f"""
            SELECT s.input_format, s.output_format, s.location, s.serde_id,s.cd_id
            FROM sds s
            WHERE s.sd_id = {s_id};
        """

        # query = f"""
        #     SELECT s.input_format, s.output_format, s.location, s.serde_id
        #     FROM sds s
        #     WHERE s.sd_id = (
        #       SELECT sd_id FROM tbls WHERE tbl_id = {tbl_id}
        #     );
        # """

        df = spark.sql(query)

        # Collect data
        row = df.collect()

        if not row:
            return None  # Return None if no data found

        # Extract the first row
        row = row[0]

        # Create an instance of the dataclass with the retrieved values
        return TableStorageInfo(
            input_format=row["input_format"],
            output_format=row["output_format"],
            location=row["location"],
            serde_id=row["serde_id"],
            cd_id=row["cd_id"]
        )

    @staticmethod
    def get_serde_metadata(spark: SparkSession, serde_id: int) -> SerdeMetadata:
        query = f"""
            SELECT ser.slib, ser.name
            FROM serdes ser
            WHERE ser.serde_id = {serde_id};
        """

        df = spark.sql(query)

        # Collect results
        row = df.collect()

        if not row:
            return None  # Return None if no data found

        # Extract the first row
        row = row[0]

        # Create an instance of the SerdeMetadata dataclass with the retrieved values
        return SerdeMetadata(
            slib=row["slib"],
            name=row["name"]
        )

    @staticmethod
    def get_serde_param(spark: SparkSession, serde_id: int) -> List[SerdeParam]:
        query = f"""
            SELECT param_key, param_value
            FROM serde_params ser
            WHERE ser.serde_id = {serde_id};
        """

        df = spark.sql(query)

        # Collect results
        rows = df.collect()

        if not rows:
            return []  # Return an empty list if no params found

        # Convert rows into a list of TableParam objects
        return [SerdeParam(
            param_key=row["param_key"],
            param_value=row["param_value"]
        ) for row in rows]

    @staticmethod
    def get_table_params(spark: SparkSession, tbl_id: int) -> List[TableParam]:
        query = f"""
            SELECT param_key, param_value
            FROM table_params
            WHERE tbl_id = {tbl_id};
        """

        df = spark.sql(query)

        # Collect results
        rows = df.collect()

        if not rows:
            return []  # Return an empty list if no params found

        # Convert rows into a list of TableParam objects
        return [TableParam(
            param_key=row["param_key"],
            param_value=row["param_value"]
        ) for row in rows]

    @staticmethod
    def get_partition_keys(spark: SparkSession, tbl_id: int) -> List[PartitionKeyMetadata]:
        query = f"""
            SELECT pk.pkey_name, pk.pkey_type, pk.pkey_comment
            FROM partition_keys pk
            WHERE pk.tbl_id = {tbl_id} order by INTEGER_IDX;
        """

        df = spark.sql(query)

        # Collect results
        rows = df.collect()

        if not rows:
            return []  # Return an empty list if no partition keys found

        # Convert rows into a list of PartitionKeyMetadata objects
        return [PartitionKeyMetadata(
            pkey_name=row["pkey_name"],
            pkey_type=row["pkey_type"],
            pkey_comment=row["pkey_comment"] or ""  # Handle NULL comments
        ) for row in rows]

    @staticmethod
    def get_hive_view_sql(spark, database_name, view_name):
        """
        Fetches the CREATE VIEW SQL statement for a given Hive view.

        :param spark: SparkSession
        :param database_name: Hive database name
        :param view_name: Hive view name
        :return: View SQL statement as a string
        """
        query = f"""
        SELECT 
            d.NAME AS database_name,
            t.TBL_NAME AS view_name,
            t.VIEW_EXPANDED_TEXT AS view_sql
        FROM TBLS t
        JOIN DBS d ON t.DB_ID = d.DB_ID
        WHERE t.TBL_TYPE = 'VIRTUAL_VIEW'
        AND d.NAME = '{database_name}'
        AND t.TBL_NAME = '{view_name}'
        """

        # Execute the query in Spark SQL
        df = spark.sql(query)

        # Extract the view SQL as a string
        result = df.collect()

        if result:
            return f"CREATE OR REPLACE VIEW {database_name}.{view_name} AS\n{result[0]['view_sql']};"
        else:
            return f"View {database_name}.{view_name} not found."
