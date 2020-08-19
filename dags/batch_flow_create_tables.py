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
        'Setup_Batch_Example1',
        'catchup=False',
        default_args=default_args,
        schedule_interval=datetime.timedelta(days=1)) as dag:


    sql1 = """ 
                CREATE or REPLACE TABLE `""" + Variable.get("gcp_project") + """`.warehouselab.sales (
            ORDERNUMBER INT64,
            QUANTITYORDERED INT64,
            SALES FLOAT64,
            ORDERDATE DATE,
            ITEM STRING
        );
        """

    sql2 = """ 
                        CREATE OR REPLACE TABLE `""" + Variable.get("gcp_project") + """`.warehouselab.sales_stg (
            ORDERNUMBER INT64,
            QUANTITYORDERED INT64,
            SALES FLOAT64,
            ORDERDATE DATE,
            ITEM STRING
        );
        """

    sql3 = """ 
                       CREATE OR REPLACE TABLE `""" + Variable.get("gcp_project") + """`.warehouselab.control_table (
            day_of_week STRING,
            process BOOLEAN,
            state STRING
        );
        """

    sqlsat = """ 
            INSERT INTO `""" + Variable.get("gcp_project") + """`.warehouselab.control_table (day_of_week,process,state) VALUES ('Saturday',false,'READY');
        """

    sqlsun = """ 
               INSERT INTO `""" + Variable.get("gcp_project") + """`.warehouselab.control_table (day_of_week,process,state) VALUES ('Sunday',false,'READY');
           """
    sqlmon = """ 
               INSERT INTO `""" + Variable.get("gcp_project") + """`.warehouselab.control_table (day_of_week,process,state) VALUES ('Monday',true,'READY');
           """
    sqltue = """ 
               INSERT INTO `""" + Variable.get("gcp_project") + """`.warehouselab.control_table (day_of_week,process,state) VALUES ('Tuesday',true,'READY');
           """
    sqlwed = """ 
               INSERT INTO `""" + Variable.get("gcp_project") + """`.warehouselab.control_table (day_of_week,process,state) VALUES ('Wednesday',true,'READY');
                   """
    sqlthur = """ 
             INSERT INTO `""" + Variable.get("gcp_project") + """`.warehouselab.control_table (day_of_week,process,state) VALUES ('Thursday',true,'READY');
           """

    sqlfri = """ 
             INSERT INTO `""" + Variable.get("gcp_project") + """`.warehouselab.control_table (day_of_week,process,state) VALUES ('Friday',true,'READY');
           """

    build_sales_table_stg = BigQueryOperator(
        task_id='build_sales_table_stg',
        bql=sql1,
        bigquery_conn_id='bigquery_default',
        use_legacy_sql=False,
        dag=dag
    )

    build_sales_table = BigQueryOperator(
        task_id='build_sales_table',
        bql=sql2,
        bigquery_conn_id='bigquery_default',
        use_legacy_sql=False,
        dag=dag
    )

    build_control_table = BigQueryOperator(
        task_id='build_control_table',
        bql=sql3,
        bigquery_conn_id='bigquery_default',
        use_legacy_sql=False,
        dag=dag
    )

    loadsat = BigQueryOperator(
        task_id='loadsat',
        bql=sqlsat,
        bigquery_conn_id='bigquery_default',
        use_legacy_sql=False,
        dag=dag
    )

    loadsun = BigQueryOperator(
        task_id='loadsun',
        bql=sqlsun,
        bigquery_conn_id='bigquery_default',
        use_legacy_sql=False,
        dag=dag
    )

    loadmon = BigQueryOperator(
        task_id='loadmon',
        bql=sqlmon,
        bigquery_conn_id='bigquery_default',
        use_legacy_sql=False,
        dag=dag
    )

    loadtue = BigQueryOperator(
        task_id='loadtue',
        bql=sqltue,
        bigquery_conn_id='bigquery_default',
        use_legacy_sql=False,
        dag=dag
    )

    loadwed = BigQueryOperator(
        task_id='loadwed',
        bql=sqlwed,
        bigquery_conn_id='bigquery_default',
        use_legacy_sql=False,
        dag=dag
    )

    loadthur = BigQueryOperator(
        task_id='loadthur',
        bql=sqlthur,
        bigquery_conn_id='bigquery_default',
        use_legacy_sql=False,
        dag=dag
    )

    loadfri = BigQueryOperator(
        task_id='loadfri',
        bql=sqlfri,
        bigquery_conn_id='bigquery_default',
        use_legacy_sql=False,
        dag=dag
    )

build_sales_table_stg >> build_sales_table >> build_control_table >> loadsat >> loadsun >> loadmon >> loadtue >> loadwed >> loadthur >> loadfri
