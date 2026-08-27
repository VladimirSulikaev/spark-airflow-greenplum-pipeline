import os
import uuid

from pyspark.sql import SparkSession


RAW_DATA_PATH = os.getenv("RAW_DATA_PATH", "s3a://raw-data")
REPORTS_PATH = os.getenv("REPORTS_PATH", "s3a://data-project/reports")


def build_spark_session(job_name: str) -> SparkSession:
    required_variables = (
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "S3_ENDPOINT",
        "S3_REGION",
    )
    missing = [name for name in required_variables if not os.getenv(name)]
    if missing:
        raise RuntimeError(f"Missing environment variables: {', '.join(missing)}")

    return (
        SparkSession.builder
        .appName(f"{job_name}-{uuid.uuid4().hex}")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2")
        .config("spark.hadoop.fs.s3a.endpoint", os.environ["S3_ENDPOINT"])
        .config("spark.hadoop.fs.s3a.region", os.environ["S3_REGION"])
        .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
        .getOrCreate()
    )

