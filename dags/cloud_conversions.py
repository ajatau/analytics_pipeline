from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import requests
from requests.auth import HTTPBasicAuth
import psycopg2
import json
import os

# Define the PostgreSQL connection and table creation function
def create_tables():
    # Connect to PostgreSQL
    POSTGRES_CONN = {
            'dbname': os.getenv('POSTGRES_DB', 'marketplace'),
            'user': os.getenv('POSTGRES_USER', 'warehouse'),
            'password': os.getenv('POSTGRES_PASSWORD', 'warehouse'),
            'host': os.getenv('POSTGRES_HOST', 'postgres-warehouse'),
            'port': os.getenv('POSTGRES_PORT', '5432')
        }
    conn = psycopg2.connect(**POSTGRES_CONN)
    cursor = conn.cursor()

    # Create total_metrics_cloud_conversion table if it doesn't exist
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS total_metrics_cloud_conversion (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255),
        dataset_name VARCHAR(255),
        series_name VARCHAR(255),
        unique_total INT,
        date DATE,
        count INT,
        UNIQUE (name, dataset_name, series_name, date)  -- Ensures no duplicates
    )
    """)

    # Create addons_cloud_conversions table if it doesn't exist
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS addons_cloud_conversions (
        id SERIAL PRIMARY KEY,
        addon_key VARCHAR(255),
        name VARCHAR(255),
        dataset_name VARCHAR(255),
        series_name VARCHAR(255),
        unique_total INT,
        date DATE,
        count INT,
        UNIQUE (addon_key, name, dataset_name, series_name, date)  -- Ensures no duplicates
    )
    """)

    # Commit the transaction and close the connection
    conn.commit()
    cursor.close()
    conn.close()

# Define the PostgreSQL insertion function
def insert_data_to_postgres(**kwargs):
    # API details
    ATLASSIAN_API_EMAIL=os.getenv('ATLASSIAN_API_EMAIL')
    ATLASSIAN_API_TOKEN= os.getenv('ATLASSIAN_API_TOKEN')
    vendor_id = "1212980"  # Replace with actual vendor ID
    url = f"https://marketplace.atlassian.com/rest/2/vendors/{vendor_id}/reporting/sales/metrics/churn?aggregation=month"
    auth = HTTPBasicAuth(ATLASSIAN_API_EMAIL, ATLASSIAN_API_TOKEN)  # Replace with your email and API token
    headers = {"Accept": "application/json"}

    # Make the API request
    response = requests.get(url, headers=headers, auth=auth)

    # Check for successful response
    if response.status_code == 200:
        api_response = response.json()  # Parse the JSON response

        # Connect to PostgreSQL
        # PostgreSQL connection parameters
        POSTGRES_CONN = {
            'dbname': os.getenv('POSTGRES_DB', 'marketplace'),
            'user': os.getenv('POSTGRES_USER', 'warehouse'),
            'password': os.getenv('POSTGRES_PASSWORD', 'warehouse'),
            'host': os.getenv('POSTGRES_HOST', 'postgres-warehouse'),
            'port': os.getenv('POSTGRES_PORT', '5432')
        }
       
        conn = psycopg2.connect(**POSTGRES_CONN)
        cursor = conn.cursor()

        # Load total metrics
        for dataset in api_response['total']['datasets']:
            dataset_name = dataset['name']
            for series in dataset['series']:
                series_name = series['name']
                unique_total = series['uniqueTotal']
                for element in series['elements']:
                    date = element['date']
                    count = element['count']
                    
                    # Check if the record already exists
                    cursor.execute(
                        "SELECT COUNT(*) FROM total_metrics_cloud_conversion WHERE name=%s AND dataset_name=%s AND series_name=%s AND date=%s",
                        (api_response['total']['name'], dataset_name, series_name, date)
                    )
                    exists = cursor.fetchone()[0]

                    if exists == 0:  # If the record does not exist, insert it
                        cursor.execute(
                            "INSERT INTO total_metrics_cloud_conversion (name, dataset_name, series_name, unique_total, date, count) VALUES (%s, %s, %s, %s, %s, %s)",
                            (api_response['total']['name'], dataset_name, series_name, unique_total, date, count)
                        )

        # Load addons
        for addon in api_response['addons']:
            addon_key = addon['addonKey']
            addon_name = addon['name']
            for dataset in addon['datasets']:
                dataset_name = dataset['name']
                for series in dataset['series']:
                    series_name = series['name']
                    unique_total = series['uniqueTotal']
                    for element in series['elements']:
                        date = element['date']
                        count = element['count']
                        
                        # Check if the record already exists
                        cursor.execute(
                            "SELECT COUNT(*) FROM addons_cloud_conversions WHERE addon_key=%s AND name=%s AND dataset_name=%s AND series_name=%s AND date=%s",
                            (addon_key, addon_name, dataset_name, series_name, date)
                        )
                        exists = cursor.fetchone()[0]

                        if exists == 0:  # If the record does not exist, insert it
                            cursor.execute(
                                "INSERT INTO addons_cloud_conversions (addon_key, name, dataset_name, series_name, unique_total, date, count) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                                (addon_key, addon_name, dataset_name, series_name, unique_total, date, count)
                            )

        # Commit the transaction and close the connection
        conn.commit()
        cursor.close()
        conn.close()

        print("Data loaded successfully!")
    else:
        print(f"Error: {response.status_code} - {response.text}")

# Define the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='fetch_and_insert_churn_data',
    default_args=default_args,
    description='A simple DAG to fetch churn data from an API and insert into PostgreSQL',
    schedule_interval='@monthly',  # Runs once a month
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Task to create tables
    create_tables_task = PythonOperator(
        task_id='create_cloud_conversion_tables',
        python_callable=create_tables,
        provide_context=True,
    )

    # Task to fetch and insert data
    fetch_and_insert_task = PythonOperator(
        task_id='fetch_and_insert_cloud_conversion_data',
        python_callable=insert_data_to_postgres,
        provide_context=True,
    )

    # Set task dependencies
    create_tables_task >> fetch_and_insert_task
