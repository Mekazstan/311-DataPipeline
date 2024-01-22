#!/bin/bash

# Step 1
airflow db reset -y

# Step 2
sleep 5

# Step 3
airflow db init

# Step 4
sleep 10

# Step 5
airflow users create --username admin --firstname Stan --lastname Administrator --role Admin --email mekastans@gmail.com

# Step 6
sleep 10

# Step 7
airflow users list

# Step 8
sleep 5

# Step 9
airflow webserver --daemon

# Step 10
sleep 7

# Step 11
airflow scheduler --daemon
