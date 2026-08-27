from pyspark.sql import functions as F

from common import RAW_DATA_PATH, REPORTS_PATH, build_spark_session


def main() -> None:
    spark = build_spark_session("lineitems-report")
    try:
        lineitems = spark.read.parquet(f"{RAW_DATA_PATH}/lineitem")

        result = (
            lineitems
            .groupBy("L_ORDERKEY")
            .agg(
                F.count("*").alias("items_count"),
                F.sum("L_EXTENDEDPRICE").alias("extended_price_sum"),
                F.avg("L_DISCOUNT").alias("avg_discount"),
                F.avg("L_TAX").alias("avg_tax"),
                F.avg(F.datediff("L_RECEIPTDATE", "L_SHIPDATE")).alias("delivery_days"),
                F.sum(F.when(F.col("L_RETURNFLAG") == "A", 1).otherwise(0)).alias("flag_a_count"),
                F.sum(F.when(F.col("L_RETURNFLAG") == "R", 1).otherwise(0)).alias("flag_r_count"),
                F.sum(F.when(F.col("L_RETURNFLAG") == "N", 1).otherwise(0)).alias("flag_n_count"),
            )
            .orderBy("L_ORDERKEY")
        )

        result.write.mode("overwrite").parquet(f"{REPORTS_PATH}/lineitems_report")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

