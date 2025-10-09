import airflow
from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime
from datetime import timedelta
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocStartClusterOperator,
    DataprocStopClusterOperator,
    DataprocSubmitJobOperator,
)



# define the variables
PROJECT_ID = "myprojectudemy-471808"
REGION = "us-east1"
CLUSTER_NAME = "my-demo-cluster"
COMPOSER_BUCKET = "us-east1-mycomposer-9d100398-bucket"


# PySpark scripts in GCS (job name, script path)
PYSPARK_SCRIPTS = [
    ("retailer_mysql_to_landing", "Notebooks/Landing/retailerMysqlToLanding.py"),
    ("supplier_mysql_to_landing", "Notebooks/Landing/supplierMysqlToLanding.py"),
    ("customer_reviews_api", "Notebooks/Landing/CustomerReviews_API.py"),
    ("categories_silverlayer", "Notebooks/Silverlayer/categories_silverlayer.py"),
    ("Customersilverlayer", "Notebooks/Silverlayer/Customersilverlayer.py"),
    ("order_items_silverlayer", "Notebooks/Silverlayer/order_items_silverlayer.py"),
    ("order_silverlayer", "Notebooks/Silverlayer/order_silverlayer.py"),
    ("product_supplier_silverlayer", "Notebooks/Silverlayer/product_supplier_silverlayer.py"),
    ("products_silverlayer", "Notebooks/Silverlayer/products_silverlayer.py"),
    ("suppliers_silverlayer", "Notebooks/Silverlayer/suppliers_silverlayer.py"),
    ("sale_summary_gold", "Notebooks/Goldlayer/sale_summary_gold.py")
    # Add more scripts here as needed
]

ARGS = {
    "owner": "SHAIK SAIDHUL",
    "start_date": days_ago(1),  # Set to a fixed past date for testing
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": ["***@gmail.com"],
    "email_on_success": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="pyspark_dag_dynamic",
    schedule_interval=None,
    description="DAG to dynamically run PySpark jobs on Dataproc",
    default_args=ARGS,
    catchup=False,
    tags=["pyspark", "dataproc", "etl", "dynamic"]
) as dag:

    # Start the cluster
    start_cluster = DataprocStartClusterOperator(
        task_id="start_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    previous_task = start_cluster  # To chain tasks linearly

    # Dynamically create PySpark job tasks
    for job_name, script_path in PYSPARK_SCRIPTS:
        job_file_uri = f"gs://{COMPOSER_BUCKET}/{script_path}"

        job_config = {
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {"main_python_file_uri": job_file_uri},
        }

        submit_job = DataprocSubmitJobOperator(
            task_id=f"pyspark_task_{job_name}",
            job=job_config,
            region=REGION,
            project_id=PROJECT_ID,
        )

        previous_task >> submit_job  # Set task dependency
        previous_task = submit_job  # Update for next chaining

    # Stop the cluster
    stop_cluster = DataprocStopClusterOperator(
        task_id="stop_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    previous_task >> stop_cluster
