import datetime
from datetime import date
import calendar
import airflow
from airflow import models
from airflow.operators import bash_operator
from airflow.operators import PythonOperator
from airflow.operators import BranchPythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.gcs_to_gcs import GoogleCloudStorageToGoogleCloudStorageOperator
from airflow.operators import DummyOperator
from airflow.operators import email_operator
from airflow.models import Variable
# Use airflow operators for bulk generic functions. When you need to create specific detailed functions leverage python.
# NOTE you will need to import the packages via the gpc console under pypi packages section. Not under airflow
from google.cloud.storage import Blob
from google.cloud import storage
from google.cloud import bigquery
from airflow.utils.trigger_rule import TriggerRule

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

inbound_bucket = "inbound"
str_bucket=Variable.get("lab_top_bucket")
bq_source_dir = Variable.get("lab_top_bucket") + "/inbound"
bq_target_table =  Variable.get("gcp_project") + ".warehouselab.sales_stg"

default_args = {
    'owner': 'samples',
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1),
    'start_date': YESTERDAY,
    'name': 'test',
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}


with airflow.DAG(
        'Reset_Batch_Control_table_Example1',
        'catchup=False',
        default_args=default_args,
        schedule_interval=None,
        catchup=False   )    as dag:


    sql1 = """ 
          update `""" + Variable.get("gcp_project") + """`.warehouselab.control_table
            set state = 'READY' where 1=1;
        """

    reset = BigQueryOperator(
        task_id='reset_control_table',
        bql=sql1,
        bigquery_conn_id='bigquery_default',
        use_legacy_sql=False,
        dag=dag
    )

reset