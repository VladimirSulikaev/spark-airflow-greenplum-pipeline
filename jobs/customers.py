from pyspark.sql import functions as F

from common import RAW_DATA_PATH, REPORTS_PATH, build_spark_session


def main() -> None:
    spark = build_spark_session("customers-report")
    try:
        customers = spark.read.parquet(f"{RAW_DATA_PATH}/customer")
        nations = spark.read.parquet(f"{RAW_DATA_PATH}/nation")
        regions = spark.read.parquet(f"{RAW_DATA_PATH}/region")

        result = (
            customers
            .join(nations, customers.C_NATIONKEY == nations.N_NATIONKEY, "left")
            .join(regions, nations.N_REGIONKEY == regions.R_REGIONKEY, "left")
            .groupBy("R_NAME", "N_NAME", "C_MKTSEGMENT")
            .agg(
                F.countDistinct("C_CUSTKEY").alias("unique_customers_count"),
                F.avg("C_ACCTBAL").alias("avg_acctbal"),
                F.min("C_ACCTBAL").alias("min_acctbal"),
                F.max("C_ACCTBAL").alias("max_acctbal"),
            )
            .orderBy("R_NAME", "N_NAME", "C_MKTSEGMENT")
        )

        result.write.mode("overwrite").parquet(f"{REPORTS_PATH}/customers_report")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

