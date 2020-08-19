# airflowbatchexample

Prerequisite 

From the Cloud console, go to the PYPI PACKAGES tab. Add the following packages:

Required libraries from the Python Package Index (PyPI)

Name
google.cloud.storage ==1.30.0
google.cloud.bigquery ==1.26.1
requests ==2.24.0

When you save, this will take some time for the packages to install ~10min.

========================================================================================

1. Clone repository to local cloudshell environment
git clone https://github.com/lpfrnk/airflowbatchexample.git

2. Create a local bucket.
gsutil mb gs://airflowbatchexample

3. Copy the sample data to an inbound folder
gsutil cp *.csv gs://airflowbatchexample/inbound/

4. In Airflow create a variable named: lab_top_bucket
enter the bucket name: airflowbatchexample
create a variable named: to_email
enter the recipient email: xxxx@gmail.com

This assumes that sendgrid is setup as documented.


5. Copy the DAG py files to the Airflow DAG directory

gsutil cp /home/lpfrnk/airflowbatchexample/dags/*.py gs://YOUR-AIRFLOW-DIRECTORY/dags

6. Run the DAG named Setup_Batch_Example1. This will create a dataset named warehouselab and create 3 tables (sales, sales_stg,control_table). When this is complete, the control table is loaded with 7 rows of data.

7. Run the main DAG named Batch_Example1. This will now examine the control table, then look at the inbound folder for files and when complete sets the control_table to DONE.

8. To rerun. Reset the control_table with the DAG named Reset_Batch_Control_table_Example1. and copy the files into the inbound directory. IE: gsutil cp *.csv gs://airflowbatchexample/inbound/

Overview

![alt text](https://github.com/lpfrnk/airflowbatchexample/blob/master/airflowtobigquery.jpg)
