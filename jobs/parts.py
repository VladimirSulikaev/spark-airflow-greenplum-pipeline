from pyspark.sql import functions as F

from common import RAW_DATA_PATH, REPORTS_PATH, build_spark_session


def main() -> None:
    spark = build_spark_session("parts-report")
    try:
        parts = spark.read.parquet(f"{RAW_DATA_PATH}/part")
        part_suppliers = spark.read.parquet(f"{RAW_DATA_PATH}/partsupp")
        suppliers = spark.read.parquet(f"{RAW_DATA_PATH}/supplier")
        nations = spark.read.parquet(f"{RAW_DATA_PATH}/nation")

        result = (
            part_suppliers
            .join(parts, part_suppliers.PS_PARTKEY == parts.P_PARTKEY, "left")
            .join(suppliers, part_suppliers.PS_SUPPKEY == suppliers.S_SUPPKEY, "left")
            .join(nations, suppliers.S_NATIONKEY == nations.N_NATIONKEY, "left")
            .groupBy("N_NAME", "P_TYPE", "P_CONTAINER")
            .agg(
                F.countDistinct("P_PARTKEY").alias("parts_count"),
                F.avg("P_RETAILPRICE").alias("avg_retail_price"),
                F.sum("P_SIZE").alias("total_size"),
                F.min("P_RETAILPRICE").alias("min_retail_price"),
                F.max("P_RETAILPRICE").alias("max_retail_price"),
                F.avg("PS_SUPPLYCOST").alias("avg_supply_cost"),
                F.min("PS_SUPPLYCOST").alias("min_supply_cost"),
                F.max("PS_SUPPLYCOST").alias("max_supply_cost"),
            )
            .orderBy("N_NAME", "P_TYPE", "P_CONTAINER")
        )

        result.write.mode("overwrite").parquet(f"{REPORTS_PATH}/parts_report")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

