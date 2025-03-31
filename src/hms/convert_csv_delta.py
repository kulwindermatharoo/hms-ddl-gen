from pyspark.sql import SparkSession

from src.hms.common_utils import CommonUtils

if __name__ == '__main__':




    spark = SparkSession.builder \
        .appName("UDP") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .getOrCreate()

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
    file_path = f"{project_root}/hms_prod_input"

    for k, v in file_name_table.items():
        file_full_path = f"{file_path}/hms_csv_data/{k}"  # Proper string formatting

        df = spark.read.option("header", True) \
            .option("multiLine", True) \
            .option("escape", '"') \
            .csv(file_full_path)

        # filter for only one date
        df = df.filter(" SRC_RUN_DT = '2025-01-27' ")
        delta_full_path = f"{file_path}/{v}"
        df.write.format("delta").save(delta_full_path)


    print("generated_statement")

    #spark.sql("select distinct tbl_type from tbls").show()

    spark.stop()
