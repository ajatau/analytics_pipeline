from airflow import DAG
from airflow.decorators import task
from airflow.datasets import Dataset
from airflow.utils.dates import days_ago
from typing import Optional, Literal
from datetime import timedelta
import requests
import logging
import os
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
from requests.auth import HTTPBasicAuth
import numpy as np
from pathlib import Path

# Configure logging
logger = logging.getLogger(__name__)

# Constants with proper path handling
BASE_DIR = "/opt/airflow/data"
LICENSES_FILE = os.path.join(BASE_DIR, "licenses.csv")
TRANSACTIONS_FILE = os.path.join(BASE_DIR, "transactions.csv")

# Create Datasets for dependency management
licenses_dataset = Dataset(LICENSES_FILE)
transactions_dataset = Dataset(TRANSACTIONS_FILE)

# PostgreSQL connection parameters
POSTGRES_CONN = {
    'dbname': os.getenv('POSTGRES_DB', 'marketplace'),
    'user': os.getenv('POSTGRES_USER', 'warehouse'),
    'password': os.getenv('POSTGRES_PASSWORD', 'warehouse'),
    'host': os.getenv('POSTGRES_HOST', 'postgres-warehouse'),
    'port': os.getenv('POSTGRES_PORT', '5432')
}

class DatabaseError(Exception):
    """Custom exception for database related errors"""
    pass


CREATE_TRANSACTIONS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS transactions (
    saleDate TIMESTAMP,
    transactionId TEXT,
    addonLicenseId TEXT,
    hostLicenseId TEXT,
    licenseId TEXT,
    addonName TEXT,
    addonKey TEXT,
    licenseType TEXT,
    hosting TEXT,
    changeInBillingPeriod TEXT,
    oldBillingPeriod TEXT,
    billingPeriod TEXT,
    changeInTier TEXT,
    oldTier TEXT,
    tier TEXT,
    appEntitlementId TEXT,
    appEntitlementNumber TEXT,
    parentProductName TEXT,
    changeInParentProductEdition TEXT,
    oldParentProductEdition TEXT,
    parentProductEdition TEXT,
    hostEntitlementId TEXT,
    hostEntitlementNumber TEXT,
    cloudId TEXT,
    maintenanceStartDate DATE,
    maintenanceEndDate DATE,
    saleType TEXT,
    purchasePrice NUMERIC,
    vendorAmount NUMERIC,
    partnerDiscountAmount NUMERIC,
    paymentStatus TEXT,
    dunningStatus TEXT,
    refundReason TEXT,
    creditNoteReason TEXT,
    loyaltyDiscountAmount NUMERIC,
    loyaltyDiscountReason TEXT,
    marketplacePromotionDiscountAmount NUMERIC,
    marketplacePromotionDiscountCode TEXT,
    marketplacePromotionDiscountReason TEXT,
    expertDiscountAmount NUMERIC,
    expertDiscountReason TEXT,
    manualDiscountAmount NUMERIC,
    manualDiscountReason TEXT,
    paymentTerms TEXT,
    originalTransactionId TEXT,
    originalTransactionSaleDate TIMESTAMP,
    company TEXT,
    country TEXT,
    region TEXT,
    technicalContactName TEXT,
    technicalContactEmail TEXT,
    billingContactName TEXT,
    billingContactEmail TEXT,
    partnerType TEXT,
    partnerName TEXT,
    partnerContactEmail TEXT,
    partnerContactName TEXT,
    lastUpdated TIMESTAMP,
    transactionLineItemId TEXT UNIQUE ,  -- Set as primary key
    appEdition TEXT
);
"""

CREATE_LICENSES_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS licenses (
    id SERIAL PRIMARY KEY,  -- Added the id column as SERIAL PRIMARY KEY
    addonLicenseId TEXT,
    hostLicenseId TEXT,
    licenseId TEXT,
    addonKey TEXT,
    addonName TEXT,
    licenseType TEXT,
    maintenanceStartDate DATE,
    maintenanceEndDate DATE,
    status TEXT,
    hosting TEXT,
    tier TEXT,
    company TEXT,
    country TEXT,
    region TEXT,
    technicalContactName TEXT,
    technicalContactEmail TEXT,
    technicalContactPhone TEXT,
    technicalContactAddress1 TEXT,
    technicalContactAddress2 TEXT,
    technicalContactCity TEXT,
    technicalContactState TEXT,
    technicalContactPostcode TEXT,
    billingContactName TEXT,
    billingContactEmail TEXT,
    billingContactPhone TEXT,
    billingContactAddress1 TEXT,
    billingContactAddress2 TEXT,
    billingContactCity TEXT,
    billingContactState TEXT,
    billingContactPostcode TEXT,
    partnerType TEXT,
    partnerName TEXT,
    partnerContactName TEXT,
    partnerContactEmail TEXT,
    lastUpdated TIMESTAMP,
    appEntitlementId TEXT,
    appEntitlementNumber TEXT,
    hostEntitlementId TEXT,
    hostEntitlementNumber TEXT,
    cloudId TEXT,
    cloudSiteHostname TEXT,
    cmtStatus TEXT,
    cmtRelatedOnPremLicense TEXT,
    extendedServerSupport BOOLEAN,
    licenseSourceType TEXT,
    transactionAccountId TEXT,
    appEdition TEXT,
    upgradeEvaluationEdition TEXT,
    upgradeEvaluationStartDate DATE,
    upgradeEvaluationEndDate DATE
);
"""

def get_db_connection():
    """Create and return a PostgreSQL database connection."""
    try:
        conn = psycopg2.connect(**POSTGRES_CONN)
        conn.autocommit = True
        return conn
    except psycopg2.Error as e:
        raise DatabaseError(f"Failed to connect to PostgreSQL: {str(e)}")

def download_from_api(vendor_id: str, auth: HTTPBasicAuth, data_type: Literal["licenses", "transactions"]) -> requests.Response:
    """Download data from Atlassian API."""
    endpoints = {
        "licenses": "reporting/licenses/export",
        "transactions": "reporting/sales/transactions/export"
    }
    
    if data_type not in endpoints:
        raise ValueError(f"Invalid data_type: {data_type}")
    
    base_url = "https://marketplace.atlassian.com/rest/2/vendors"
    url = f"{base_url}/{vendor_id}/{endpoints[data_type]}"
    
    try:
        logger.info(f"Downloading {data_type} data from {url}")
        response = requests.get(
            url,
            auth=auth,
            headers={"Accept": "text/csv"},
            timeout=30
        )
        response.raise_for_status()
        return response
    except requests.exceptions.RequestException as e:
        raise Exception(f"Failed to download {data_type}: {str(e)}")

def save_response_to_file(response: requests.Response, file_path: str) -> None:
    """Save API response content to a file."""
    try:
        directory = os.path.dirname(file_path)
        Path(directory).mkdir(parents=True, exist_ok=True)
        with open(file_path, 'wb') as file:
            file.write(response.content)
        logger.info(f"Successfully saved data to {file_path}")
    except Exception as e:
        logger.exception(f"Failed to save file: {file_path}")
        raise Exception(f"Failed to save file: {str(e)}")

@task(outlets=[licenses_dataset])
def download_licenses(vendor_id: Optional[str] = "1212980") -> None:
    """Download license file from Atlassian API."""
    try:
        api_email = os.getenv('ATLASSIAN_API_EMAIL')
        api_token = os.getenv('ATLASSIAN_API_TOKEN')
        auth = HTTPBasicAuth(api_email, api_token)
        response = download_from_api(vendor_id, auth, "licenses")
        save_response_to_file(response, LICENSES_FILE)
    except Exception as e:
        logger.exception("Error in download_licenses task")
        raise

@task(outlets=[transactions_dataset])
def download_transactions(vendor_id: Optional[str] = "1212980") -> None:
    """Download transactions file from Atlassian API."""
    try:
        api_email = os.getenv('ATLASSIAN_API_EMAIL')
        api_token = os.getenv('ATLASSIAN_API_TOKEN')
        auth = HTTPBasicAuth(api_email, api_token)
        response = download_from_api(vendor_id, auth, "transactions")
        save_response_to_file(response, TRANSACTIONS_FILE)
    except Exception as e:
        logger.exception("Error in download_transactions task")
        raise

@task
def initialize_database() -> None:
    """Initialize PostgreSQL database by ensuring table creation with the `id` column as primary key."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                # Create the licenses table with the updated schema
                cursor.execute(CREATE_LICENSES_TABLE_SQL)
                logger.info("Licenses table initialized with 'id' as primary key.")
                
                # Ensure transactions table is created
                cursor.execute(CREATE_TRANSACTIONS_TABLE_SQL)
                logger.info("Transactions table initialized successfully.")
    except Exception as e:
        logger.exception("Error initializing database")
        raise

def load_data_from_csv_to_db(file_path: str, table_name: str, date_columns: list, numeric_columns: list, boolean_columns: list, use_on_conflict: bool = False):
    """Load CSV data into PostgreSQL table with ON CONFLICT to handle duplicates."""
    try:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"{file_path} does not exist.")

        # Read the CSV file
        df = pd.read_csv(file_path)
        df = df.replace({'': None, 'null': None, 'NULL': None})

        # Convert date columns to datetime and replace NaT with None
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
                df[col] = df[col].apply(lambda x: None if pd.isna(x) else x)

        # Convert numeric columns to float and replace NaN with None
        for col in numeric_columns:
            if col in df.columns:
                df[col] = df[col].replace({np.nan: None})

        # Convert boolean columns to boolean type and replace NaN with None
        for col in boolean_columns:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: True if str(x).lower() == 'true' else (False if str(x).lower() == 'false' else None))

        # Convert DataFrame to list of tuples, ensuring no NaT or NaN values
        values = [tuple(None if pd.isna(x) else x for x in row) for row in df.values]
        columns = ','.join(df.columns)

        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                if use_on_conflict:
                    insert_stmt = f"""
                        INSERT INTO {table_name} ({columns}) VALUES %s
                        ON CONFLICT (transactionLineItemId) DO NOTHING;
                    """
                else:
                    insert_stmt = f"""
                        INSERT INTO {table_name} ({columns}) VALUES %s;
                    """
                execute_values(cursor, insert_stmt, values)

        logger.info(f"Successfully loaded data into {table_name}")


    except Exception as e:
        logger.exception(f"Error loading data into {table_name}")
        raise



@task
def load_transactions() -> None:
    """Load transactions data from CSV to PostgreSQL database."""
    load_data_from_csv_to_db(
        TRANSACTIONS_FILE, 'transactions',
        date_columns=['saleDate', 'maintenanceStartDate', 'maintenanceEndDate', 'originalTransactionSaleDate', 'lastUpdated'],
        numeric_columns=['purchasePrice', 'vendorAmount', 'partnerDiscountAmount', 'loyaltyDiscountAmount', 'marketplacePromotionDiscountAmount', 'expertDiscountAmount', 'manualDiscountAmount'],
        boolean_columns=['extendedServerSupport'],
        use_on_conflict=True  # Use ON CONFLICT f
        #primary_key='transactionLineItemId'  # Specify the primary key column
    )

@task
def load_licenses() -> None:
    """Load licenses data from CSV to PostgreSQL database."""
    load_data_from_csv_to_db(
        LICENSES_FILE, 'licenses',
        date_columns=['maintenanceStartDate', 'maintenanceEndDate', 'lastUpdated', 'upgradeEvaluationStartDate', 'upgradeEvaluationEndDate'],
        numeric_columns=[],
        boolean_columns=['extendedServerSupport'],
        use_on_conflict=False  # Use ON CONFLICT f
        #add_id=True  # Adds the `id` column as primary key
    )

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'marketplace_etl',
    default_args=default_args,
    description='Downloads Atlassian Marketplace data and loads to PostgreSQL',
    schedule='@daily',
    catchup=False,
    tags=['atlassian', 'marketplace', 'etl'],
) as dag:
    
    # Download tasks
    licenses_task = download_licenses()
    transactions_task = download_transactions()
    
    # Database initialization and loading tasks
    init_db = initialize_database()
    load_licenses_data = load_licenses()
    load_transactions_data = load_transactions()
    
    # Set task dependencies
    [licenses_task, transactions_task] >> init_db
    init_db >> [load_licenses_data, load_transactions_data]
