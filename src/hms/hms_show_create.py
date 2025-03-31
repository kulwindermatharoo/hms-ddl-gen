from pyspark.sql import SparkSession
from data_model import PartitionKeyMetadata, TableParam, SerdeMetadata, TableStorageInfo, ColumnMetadata, \
    TableMetadata, SerdeParam
from typing import List

from hms_utils import HmsUtils

import concurrent.futures
import os

from src.hms.common_utils import CommonUtils


def generate_create_table_statement(
        table_metadata: TableMetadata,
        column_metadata: List[ColumnMetadata],
        partition_keys: List[PartitionKeyMetadata],
        storage_info: TableStorageInfo,
        serde_metadata: SerdeMetadata,
        serde_params: List[SerdeParam],
        table_params: List[TableParam]
) -> str:
    # Columns part of the CREATE statement
    columns_sql = ",\n  ".join(
        [f"`{col.column_name}` {col.type_name} COMMENT '{col.comment}'" for col in column_metadata]
    )

    # Partition columns part of the CREATE statement
    partition_sql = ""
    if partition_keys:
        partition_sql = "PARTITIONED BY (\n  " + ",\n  ".join(
            [f"`{pkey.pkey_name}` {pkey.pkey_type} COMMENT '{pkey.pkey_comment}'" for pkey in partition_keys]
        ) + "\n)"

    # Table properties (TBLPROPERTIES) part of the CREATE statement
    properties_sql = ",\n  ".join(
        [f"'{param.param_key}' = '{param.param_value}'" for param in table_params]
    )
    properties_sql = f"TBLPROPERTIES (\n  {properties_sql}\n)"

    # SerdeProperties part of the CREATE statement
    # serde_properties_sql = ",\n  ".join(
    #    [f"'{param.param_key}' = '{param.param_value}'" for param in serde_params]
    # )
    # serde_properties_sql = ",\n  ".join(
    #    ["'{0}' = '{1}'".format(param.param_key, param.param_value.replace('\n', '\\n'))
    #     for param in serde_params]
    # )
    serde_properties_sql = ",\n  ".join(
        ["'{0}' = '{1}'".format(param.param_key, (param.param_value or "").replace('\n', '\\n')) for param in
         serde_params]
    )

    serde_properties_sql = f"WITH SERDEPROPERTIES (\n  {serde_properties_sql}\n)"

    # Final CREATE statement
    create_statement = f"""
    CREATE EXTERNAL TABLE `{table_metadata.database_name}`.`{table_metadata.table_name}` (
        {columns_sql}
        )
        {partition_sql}
        ROW FORMAT SERDE '{serde_metadata.slib}'
        {serde_properties_sql}
        STORED AS INPUTFORMAT '{storage_info.input_format}'
        OUTPUTFORMAT '{storage_info.output_format}'
        LOCATION '{storage_info.location}'
        {properties_sql};
        """
    return create_statement


def process_table(item):
    db_name = item['db_name']
    tb_name = item['table_name']
    output_file_name = f"{db_name}.{tb_name}.sql"

    try:
        table_metadata = HmsUtils.get_table_metadata(spark, db_name, tb_name)
        storage_info = HmsUtils.get_table_storage_info(spark, table_metadata.sd_id)
        column_list = HmsUtils.get_table_columns(spark, storage_info.cd_id)
        serde_info = HmsUtils.get_serde_metadata(spark, storage_info.serde_id)
        serde_parm_list = HmsUtils.get_serde_param(spark, storage_info.serde_id)
        table_params_list = HmsUtils.get_table_params(spark, table_metadata.tbl_id)
        partition_keys_list = HmsUtils.get_partition_keys(spark, table_metadata.tbl_id)

        generated_statement = generate_create_table_statement(
            table_metadata,
            column_list,
            partition_keys_list,
            storage_info,
            serde_info,
            serde_parm_list,
            table_params_list)

        # print(generated_statement)

        full_out_path = f"{output_path}/{output_file_name}"
        with open(full_out_path, "w") as file:
            file.write(generated_statement)
            file.flush()

        return f"Processed {db_name}.{tb_name}"

    except Exception as e:
        return f"Error processing {db_name}.{tb_name}: {e}"


if __name__ == '__main__':

    spark = SparkSession.builder \
        .appName("UDP") \
        .master("local[8]") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC") \
        .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC") \
        .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC") \
        .config("spark.executor.memory", "6g") \
        .config("spark.executor.memoryOverhead", "2g") \
        .getOrCreate()

    # spark.conf.set("spark.executor.memory", "8g")  # Increase from 4g (default)
    # spark.conf.set("spark.executor.memoryOverhead", "2g")
    # spark.conf.set("spark.executor.extraJavaOptions", "-XX:+UseG1GC")
    # spark.conf.set("spark.driver.extraJavaOptions", "-XX:+UseG1GC")

    file_name_table = {
        'HMS_COLUMNS_V2_20250210.csv': 'columns_v2',
        'HMS_PARTITION_KEYS_20250210.csv': 'partition_keys',
        'HMS_SERDES_20250210.csv': 'serdes',
        'HMS_TABLE_PARAMS_20250210.csv': 'table_params',
        'HMS_DBS_20250210.csv': 'dbs',
        'HMS_SDS_20250210.csv': 'sds',
        'HMS_SERDE_PARAMS_20250210.csv': 'serde_params',
        'HMS_TBLS_20250210.csv': 'tbls'
    }

    project_root = CommonUtils.find_project_root()
    output_path = f"{project_root}/hms_dev_output/external"
    file_path = f"{project_root}/hms_dev_input"

    # register hive metadata tables(columns_v2|partition_keys|serdes|table_params|dbs|tbls|sds|serde_params)
    # from the delta files
    for k, v in file_name_table.items():
        file_full_path = f"{file_path}/{v}"

        df = spark.read.format("delta") \
            .load(file_full_path)
        df.createOrReplaceTempView(v)

    df = spark.sql("""
        SELECT  d.NAME as db_name, TBL_NAME as table_name
        FROM tbls t
        JOIN dbs d
        ON t.db_id = d.db_id
        WHERE tbl_type IN ('EXTERNAL_TABLE')
    """)

    # df = spark.sql("""
    #        SELECT d.db_ID, d.NAME, TBL_NAME, TBL_TYPE
    #        FROM tbls t
    #        JOIN dbs d
    #        ON t.db_id = d.db_id
    #        WHERE tbl_type IN ('EXTERNAL_TABLE')
    #    """)

    rows = df.collect()
    database_table_list = [{'db_name': row['db_name'], 'table_name': row['table_name']} for row in rows]

    # df.show()

    # input dictionary of db and table_names
    database_table_dict_not_used = {
        "dlprod_curated_inv_db": "pms_prod_prfl_dly",
        "dlprod_lunar_tmp_db": "cust_acct_info_patch",
        "dlprod_persist_imex_db": "ix_ixqdwdcq_1_0",
        "dlprod_persist_lead_auto_db": "lead_auto_std_a_loanf_d",
        "dlprod_raw_enlite_db": "enlite_company_name_changing_1_0",
        "dlprod_raw_obwl_db": "obwl_trn_order_submission_1_0",
        "dlprod_raw_plps_db": "plps_inc_coll_lea_1_0",
        "dlprod_persist_mnf_speedyln_db": "mnf_speedyln_src_bo_recovery_spl_wk_1_0",
        "dlprod_raw_imex_db": "ix_ixqdwcad_1_0",
        "dlprod_raw_imexi_db": "ixi_ixqap346_1_0"
    }

    # serial execution
    # for item in database_table_list:
    #    process_table(item)

    # exit(0)

    # Number of threads in the pool
    max_threads = 9

    # Run the thread pool
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = {}
        for item in database_table_list:
            future = executor.submit(process_table, item)
            futures[future] = item

        for future in concurrent.futures.as_completed(futures):
            print(future.result())

    # spark.sql("select distinct tbl_type from tbls").show()

    spark.stop()
