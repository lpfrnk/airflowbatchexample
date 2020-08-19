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




def locate_files_in_landing_zone(**context):
    file_names = ""
    msg=""
    client = storage.Client()

    buckets = client.list_blobs
    cnt = 0
    print("full list of files: " + str(buckets))
    print ("Will examine bucket: " + str_bucket + " - " + inbound_bucket )
    for bloba in client.list_blobs(str_bucket, prefix=inbound_bucket):
        print(bloba.name)
        #file_names = file_names + " " + str(bloba.name)
        file_name = bloba.name
        if cnt == 0:
            file_names = file_name
        else:
            file_names = file_names + "\n" + file_name
        cnt = cnt + 1

    if cnt == 0:

        return "no_files_found"
    else:
        msg = file_names
        #print("message to push: '%s'" % msg)    
        task_instance = context['task_instance']
        # Here we are setting an xcom key value
        task_instance.xcom_push(key="file_names", value=msg)
        #.xcom_push(key="db_con", value=msg)
        print("files_identified:" + msg)
      
        return "files_found"


def check_control():
     

    my_date = date.today()

    day_of_wk = (calendar.day_name[my_date.weekday()])

    print ("Workday check: ", day_of_wk)
    print("workday_function start")
    client = bigquery.Client()
    print("workday_function setup client")
    sql = """
                SELECT state FROM `""" + Variable.get("gcp_project") + """.warehouselab.control_table` where 
                day_of_week = '""" + day_of_wk + """'
           """
    print("workday_function ran sql: " + sql )     
    query_job = client.query(
            sql
    )
   
    cnt = 0
    results = query_job.result()  # Waits for job to complete.
    print("workday_function results:", str(results))
    for row in results:
        print(("workday_function readrecords:", str(row.state)))
        cnt = cnt + 1
    print("workday_function counts:",cnt)
    if str(row.state) != "READY":
        return "not_ready"
    else:
        return "check_for_files"

def wait_timer():
    import time
    print ("Start : %s" % time.ctime())
    time.sleep(5)
    print ("End : %s" % time.ctime())

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
        'Batch_Example1',
        'catchup=False',
        default_args=default_args,
        schedule_interval=datetime.timedelta(days=1)) as dag:

    # Print the dag_run id from the Airflow logs
    #print_dag_run_conf = bash_operator.BashOperator(
    #    task_id='print_dag_run_conf', bash_command='echo {{ dag_run.id }}')

    #print_dag_run_conf2 = bash_operator.BashOperator(
    #    task_id='print_dag_run_conf2', bash_command='echo {{ dag_run.id }}')

    check_for_files = BranchPythonOperator(
        task_id='check_for_files',
        provide_context=True,
        python_callable=locate_files_in_landing_zone,
        dag=dag)

        # [START composer_email]
        # Send email confirmation
    files_found = email_operator.EmailOperator(
        task_id='files_found',
        to=Variable.get("to_email"),
        subject='Files Found',
        html_content= "Files located and will be processed: " '{{ ti.xcom_pull(task_ids=\'check_for_files\', '
                'key=\'file_names\') }}',
        provide_context=True)
        # [END composer_email]

    no_files_found = email_operator.EmailOperator(
        task_id='no_files_found',
        to=Variable.get("to_email"),
        subject='no_data',
        html_content="""
         No Files Found Check with the Vendor. Bucket:""" + str_bucket + "/" + inbound_bucket +
        "")
        # [END composer_email]

    start = DummyOperator(
        task_id='start',
        dag=dag,
    )

    end = DummyOperator(
        task_id='end',
        dag=dag,
    )

    sql = """   
        MERGE
              `""" + Variable.get("gcp_project") + """.warehouselab.sales` T
            USING
              `""" + Variable.get("gcp_project") + """.warehouselab.sales_dedup` S
            ON
              t.ORDERNUMBER=s.ORDERNUMBER
              WHEN MATCHED THEN UPDATE SET t.QUANTITYORDERED = s.QUANTITYORDERED
              WHEN NOT MATCHED
              THEN
            INSERT
              (ORDERNUMBER, 
			  QUANTITYORDERED, 
			  SALES, 
			  ORDERDATE, 
			  ITEM)
            VALUES
              (ORDERNUMBER, 
			  QUANTITYORDERED, 
			  SALES, 
			  ORDERDATE, 
			  ITEM)
             """
    merge_data = BigQueryOperator(
        task_id='merge_data',
        bql=sql,
        bigquery_conn_id='bigquery_default',
        use_legacy_sql=False,
        dag=dag
    )

    sql2 = """ 
        create or replace table `""" + Variable.get("gcp_project") + """.warehouselab.sales_totals` as
            select 
            round(sum(sales),2) as total_sales,
            sum(QUANTITYORDERED) as cars_ordered, 
            ORDERDATE,
            ITEM from 
              `""" + Variable.get("gcp_project") + """.warehouselab.sales`
            group by ORDERDATE,ITEM 
            order by ORDERDATE,ITEM 
        """

    build_table = BigQueryOperator(
        task_id='build_table',
        bql=sql2,
        bigquery_conn_id='bigquery_default',
        use_legacy_sql=False,
        dag=dag
    )

    sql3 = """ 
        create or replace table `""" + Variable.get("gcp_project") + """.warehouselab.sales_dedup` as
            SELECT distinct 
              ORDERNUMBER, 
			  QUANTITYORDERED, 
			  SALES, 
			  ORDERDATE, 
			  ITEM
            FROM
              `""" + Variable.get("gcp_project") + """.warehouselab.sales_stg` ; 
              """
   
    de_duplicate = BigQueryOperator(
        task_id='de_duplicate',
        bql=sql3,
        bigquery_conn_id='bigquery_default',
        use_legacy_sql=False,
        dag=dag
    )
    
    sql4 = """ 
        update `""" + Variable.get("gcp_project") + """.warehouselab.control_table`
        set state = 'COMPLETE' where 1=1;
        """
   
    set_control_table_done = BigQueryOperator(
        task_id='set_control_table_done',
        bql=sql4,
        bigquery_conn_id='bigquery_default',
        use_legacy_sql=False,
        dag=dag
    )
    

    load_data = GoogleCloudStorageToBigQueryOperator(
        task_id="staging_data",
        bucket=bq_source_dir,
        source_objects=["*"],
        destination_project_dataset_table=bq_target_table,
        write_disposition="WRITE_TRUNCATE",
        skip_leading_rows=1
    )

    move_data = GoogleCloudStorageToGoogleCloudStorageOperator(
        task_id="move_data_to_processed",
        source_bucket=Variable.get("lab_top_bucket"),
        source_object="inbound/*",
        destination_bucket=Variable.get("lab_top_bucket"),
        destination_object="processed/",
        move_object=True,
    )

    not_ready = email_operator.EmailOperator(
        task_id='not_ready',
        to=Variable.get("to_email"),
        subject='System Not Ready',
        html_content="""
            System Not Ready. Will skip processing.
            """)
    # [END composer_email]

    check_control_table_status = BranchPythonOperator(
        task_id='check_control_table_status',
        provide_context=False,
        python_callable=check_control,
        dag=dag)

    # Here we create two conditional tasks, one of which will be executed
    # based on whether the job was a success or a failure.
    success_task = email_operator.EmailOperator(task_id='success',
                                                    	trigger_rule=TriggerRule.ALL_SUCCESS,
														to=models.Variable.get('to_email'),
														subject='Job Succeeded: start_date {{ ds }}',
														html_content="HTML CONTENT"
														)

    failure_task = email_operator.EmailOperator(task_id='failure',
                                                    	trigger_rule=TriggerRule.ALL_FAILED,
														to=models.Variable.get('to_email'),
														subject='Job Failed: start_date {{ ds }}',
														html_content="HTML CONTENT"
														)


start >> check_control_table_status >> check_for_files >> files_found >> load_data >> de_duplicate >> merge_data >> build_table  >> move_data >> set_control_table_done >> success_task >> end
start >> check_control_table_status >> check_for_files >> files_found >> load_data >> de_duplicate >> merge_data >> build_table  >> move_data >> set_control_table_done >> failure_task >> end

start >> check_control_table_status >> check_for_files >> no_files_found >> end
start >> check_control_table_status >> not_ready >> end