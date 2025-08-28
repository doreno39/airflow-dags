from datetime import datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor

with DAG(
    dag_id="example_spark_on_k8s",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["spark", "kubernetes"],
) as dag:

    submit = SparkKubernetesOperator(
        task_id="submit_spark_pi",
        # Đường dẫn file SparkApplication trong DAGs (đã git-sync)
        application_file="/opt/airflow/dags/spark/spark-pi.yaml",
        namespace="data",
        do_xcom_push=True,   # để sensor biết tên ứng dụng
        delete_on_termination=True,  # xóa SparkApplication sau khi xong
    )

    monitor = SparkKubernetesSensor(
        task_id="monitor_spark_pi",
        namespace="data",
        application_name="{{ task_instance.xcom_pull(task_ids='submit_spark_pi')['metadata']['name'] }}",
        attach_log=True,
        poke_interval=30,
        timeout=60 * 30,
    )

    submit >> monitor
