import os
import re

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


REPORTS = ("customers", "lineitems", "orders", "parts", "suppliers")
K8S_NAMESPACE = os.getenv("K8S_NAMESPACE", "data-project")
K8S_CONNECTION_ID = os.getenv("K8S_CONNECTION_ID", "kubernetes_default")
GREENPLUM_CONNECTION_ID = os.getenv("GREENPLUM_CONNECTION_ID", "greenplum_default")
GREENPLUM_SCHEMA = os.getenv("GREENPLUM_SCHEMA", "data_project")
PXF_BUCKET = os.getenv("PXF_BUCKET", "data-project")
PXF_PREFIX = os.getenv("PXF_PREFIX", "reports")
DAG_DIRECTORY = os.path.dirname(os.path.abspath(__file__))

if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", GREENPLUM_SCHEMA):
    raise ValueError("GREENPLUM_SCHEMA must be a valid SQL identifier")


TABLE_COLUMNS = {
    "customers": """
        R_NAME TEXT,
        N_NAME TEXT,
        C_MKTSEGMENT TEXT,
        unique_customers_count BIGINT,
        avg_acctbal FLOAT8,
        min_acctbal FLOAT8,
        max_acctbal FLOAT8
    """,
    "lineitems": """
        L_ORDERKEY BIGINT,
        items_count BIGINT,
        extended_price_sum FLOAT8,
        avg_discount FLOAT8,
        avg_tax FLOAT8,
        delivery_days FLOAT8,
        flag_a_count BIGINT,
        flag_r_count BIGINT,
        flag_n_count BIGINT
    """,
    "orders": """
        O_MONTH TEXT,
        N_NAME TEXT,
        O_ORDERPRIORITY TEXT,
        orders_count BIGINT,
        avg_order_price FLOAT8,
        sum_order_price FLOAT8,
        min_order_price FLOAT8,
        max_order_price FLOAT8,
        status_f_count BIGINT,
        status_o_count BIGINT,
        status_p_count BIGINT
    """,
    "parts": """
        N_NAME TEXT,
        P_TYPE TEXT,
        P_CONTAINER TEXT,
        parts_count BIGINT,
        avg_retail_price FLOAT8,
        total_size BIGINT,
        min_retail_price FLOAT8,
        max_retail_price FLOAT8,
        avg_supply_cost FLOAT8,
        min_supply_cost FLOAT8,
        max_supply_cost FLOAT8
    """,
    "suppliers": """
        R_NAME TEXT,
        N_NAME TEXT,
        unique_suppliers_count BIGINT,
        avg_acctbal FLOAT8,
        min_acctbal FLOAT8,
        max_acctbal FLOAT8
    """,
}


def external_table_sql(report: str) -> str:
    return f"""
        DROP EXTERNAL TABLE IF EXISTS {GREENPLUM_SCHEMA}.{report};
        CREATE EXTERNAL TABLE {GREENPLUM_SCHEMA}.{report} (
            {TABLE_COLUMNS[report]}
        )
        LOCATION ('pxf://{PXF_BUCKET}/{PXF_PREFIX}/{report}_report?PROFILE=s3:parquet&SERVER=default')
        ON ALL FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import') ENCODING 'UTF8';
    """


with DAG(
    dag_id="spark-s3-greenplum-pipeline",
    schedule_interval=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["spark", "s3", "greenplum"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    for report in REPORTS:
        submit = SparkKubernetesOperator(
            task_id=f"submit_{report}",
            namespace=K8S_NAMESPACE,
            application_file=os.path.join(DAG_DIRECTORY, "..", "k8s", f"{report}.yaml"),
            kubernetes_conn_id=K8S_CONNECTION_ID,
            do_xcom_push=True,
        )

        sensor = SparkKubernetesSensor(
            task_id=f"sensor_{report}",
            namespace=K8S_NAMESPACE,
            application_name=(
                "{{ task_instance.xcom_pull(task_ids='submit_"
                + report
                + "')['metadata']['name'] }}"
            ),
            kubernetes_conn_id=K8S_CONNECTION_ID,
            attach_log=True,
        )

        create_table = SQLExecuteQueryOperator(
            task_id=f"create_{report}_table",
            conn_id=GREENPLUM_CONNECTION_ID,
            sql=external_table_sql(report),
            split_statements=True,
            autocommit=True,
        )

        start >> submit >> sensor >> create_table >> end
