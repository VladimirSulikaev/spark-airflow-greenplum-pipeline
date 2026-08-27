from pyspark.sql import functions as F

from common import RAW_DATA_PATH, REPORTS_PATH, build_spark_session


def main() -> None:
    spark = build_spark_session("suppliers-report")
    try:
        suppliers = spark.read.parquet(f"{RAW_DATA_PATH}/supplier")
        nations = spark.read.parquet(f"{RAW_DATA_PATH}/nation")
        regions = spark.read.parquet(f"{RAW_DATA_PATH}/region")

        result = (
            suppliers
            .join(nations, suppliers.S_NATIONKEY == nations.N_NATIONKEY, "left")
            .join(regions, nations.N_REGIONKEY == regions.R_REGIONKEY, "left")
            .groupBy("R_NAME", "N_NAME")
            .agg(
                F.countDistinct("S_SUPPKEY").alias("unique_suppliers_count"),
                F.avg("S_ACCTBAL").alias("avg_acctbal"),
                F.min("S_ACCTBAL").alias("min_acctbal"),
                F.max("S_ACCTBAL").alias("max_acctbal"),
            )
            .orderBy("R_NAME", "N_NAME")
        )

        result.write.mode("overwrite").parquet(f"{REPORTS_PATH}/suppliers_report")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

