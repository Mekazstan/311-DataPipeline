"""
_summary_
Create a data validation task using apache airflow & great expectation library
NB: Great Expectation library can only be used when the project has been initialized in theworking project directory

"""

import sys
from faker import Faker
import csv
from great_expectations import DataContext
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

# Data creation function
def create_csv_data():
    output=open('people.csv','w')
    fake=Faker()
    header=['name','age','street','city','state','zip','lng','lat']
    mywriter=csv.writer(output)
    mywriter.writerow(header)
    for r in range(1000):
        mywriter.writerow([fake.name(),fake.random_int(min=18,
        max=80, step=1), fake.street_address(), fake.city(),fake.
        state(),fake.zipcode(),fake.longitude(),fake.latitude()])
    output.close()
    
# Data validation function
def validateData():
    context = DataContext("/home/chukwuemeka/Documents/DataWithPY/311-Pipeline/gx")
    suite = context.get_expectation_suite("people.validate")
    batch_kwargs = {
        "path": "/home/chukwuemeka/Documents/DataWithPY/311-Pipeline/dags/people.csv",
        "datasource": "files_datasource",
        "reader_method": "read_csv",
    }
    batch = context.get_batch(batch_kwargs, suite)
    results = context.run_validation_operator("action_list_operator", [batch])
    if not results["success"]:
        raise AirflowException("Validation Failed")
    else:
        print("Validation ran successfully...")
        
        
# Default arguments for the DAG
default_args = {
    'owner': 'mekazstan',
    'start_date': datetime(2024, 1, 22),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'create_move_validate',
    default_args=default_args,
    description='DAG to create & populate CSV data, move it to a location and validate the data in the csv',
    schedule_interval='@daily',
)

# Task to create the CSV data
create_csv_task = PythonOperator(
    task_id='create_csv',
    python_command=create_csv_data,
    dag=dag,
)

# Task to move the CSV file
move_csv_task = BashOperator(
    task_id='move_csv',
    bash_command='mv /home/chukwuemeka/Documents/DataWithPY/311-Pipeline/dags/people.csv /home/chukwuemeka/Documents/DataWithPY/311-Pipeline/',
    dag=dag,
)

# Task to validate the data
validate_data_task = PythonOperator(
    task_id='validate_data',
    python_command=validateData,
    dag=dag,
)

# Set the task dependencies
create_csv_task >> move_csv_task >> validate_data_task
        