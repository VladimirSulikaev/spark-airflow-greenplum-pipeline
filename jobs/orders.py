from pyspark.sql import functions as F

from common import RAW_DATA_PATH, REPORTS_PATH, build_spark_session


def main() -> None:
    spark = build_spark_session("orders-report")
    try:
        orders = spark.read.parquet(f"{RAW_DATA_PATH}/orders")
        customers = spark.read.parquet(f"{RAW_DATA_PATH}/customer")
        nations = spark.read.parquet(f"{RAW_DATA_PATH}/nation")

        result = (
            orders
            .join(customers, orders.O_CUSTKEY == customers.C_CUSTKEY, "left")
            .join(nations, customers.C_NATIONKEY == nations.N_NATIONKEY, "left")
            .withColumn("O_MONTH", F.substring("O_ORDERDATE", 1, 7))
            .groupBy("O_MONTH", "N_NAME", "O_ORDERPRIORITY")
            .agg(
                F.count("O_ORDERKEY").alias("orders_count"),
                F.avg("O_TOTALPRICE").alias("avg_order_price"),
                F.sum("O_TOTALPRICE").alias("sum_order_price"),
                F.min("O_TOTALPRICE").alias("min_order_price"),
                F.max("O_TOTALPRICE").alias("max_order_price"),
                F.sum(F.when(F.col("O_ORDERSTATUS") == "F", 1).otherwise(0)).alias("status_f_count"),
                F.sum(F.when(F.col("O_ORDERSTATUS") == "O", 1).otherwise(0)).alias("status_o_count"),
                F.sum(F.when(F.col("O_ORDERSTATUS") == "P", 1).otherwise(0)).alias("status_p_count"),
            )
            .orderBy("N_NAME", "O_ORDERPRIORITY", "O_MONTH")
        )

        result.write.mode("overwrite").parquet(f"{REPORTS_PATH}/orders_report")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

