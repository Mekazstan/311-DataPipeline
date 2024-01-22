"""
_summary_
Create a data validation task using apache airflow & great expectation library
NB: Great Expectation library can only be used when 
1. The project has been initialized in the working project directory 
2. The Expectation Suite has been generated
3. The datasource has been set in great_expectations.yml
4. The run_validation_operator has been set in great_expectations.yml

"""

import sys
from faker import Faker
import csv
from great_expectations import DataContext
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import great_expectations as gx

# from great_expectations_provider.operators.great_expectations import (
#     GreatExpectationsOperator
# )

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
    # Creating a Data Context
    context = gx.get_context()
    
    # Connecting to the data
    validator = context.sources.pandas_default.read_csv(
    "/home/chukwuemeka/Documents/DataWithPY/311-Pipeline/people.csv")
    
    # Creating Expectations
    validator.expect_column_values_to_not_be_null("age")
    validator.expect_column_values_to_be_between(
        "age", min_value=15, max_value=88
    )
    validator.save_expectation_suite()
    
    # Validating data
    checkpoint = context.add_or_update_checkpoint(
        name="my_quickstart_checkpoint",
        validator=validator,
    )
    
    checkpoint_result = checkpoint.run()
    if not checkpoint_result:
        raise AirflowException("Validation Failed")
    else:
        context.view_validation_result(checkpoint_result)
        print("Validation ran successfully...")
    
    
    # context = DataContext("/home/chukwuemeka/Documents/DataWithPY/311-Pipeline/gx")
    # suite = context.get_expectation_suite("people_suite")
    # batch_kwargs = {
    #     "path": "/home/chukwuemeka/Documents/DataWithPY/311-Pipeline/people.csv",
    #     "datasource": "files_datasource",
    #     "reader_method": "read_csv",
    # }
    # batch = context._get_batch_v2(batch_kwargs, suite)
    # results = context.run_validation_operator("action_list_operator", [batch])
    # if not results["success"]:
    #     raise AirflowException("Validation Failed")
    # else:
    #     print("Validation ran successfully...")
        
        
# Default arguments for the DAG
default_args = {
    'owner': 'mekazstan',
    'start_date': datetime(2024, 1, 22),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'create_and_validate',
    default_args=default_args,
    description='DAG to create & populate CSV data, move it to a location and validate the data in the csv',
    schedule='@daily',
)

# Task to create the CSV data
create_csv_task = PythonOperator(
    task_id='create_csv',
    python_callable=create_csv_data,
    dag=dag,
)

# # Task to move the CSV file
# move_csv_task = BashOperator(
#     task_id='move_csv',
#     bash_command='mv /home/chukwuemeka/Documents/DataWithPY/311-Pipeline/dags/people.csv /home/chukwuemeka/Documents/DataWithPY/311-Pipeline/',
#     dag=dag,
# )

# Task to validate the data
validate_data_task = PythonOperator(
    task_id='validate_data',
    python_callable=validateData,
    dag=dag,
)

# Set the task dependencies
create_csv_task  >> validate_data_task
        