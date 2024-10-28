from airflow import DAG
from airflow.decorators import task
from airflow.datasets import Dataset
from airflow.utils.dates import days_ago
from typing import Optional, Literal
from datetime import timedelta
import requests
import logging
import os
import sqlite3
import pandas as pd
from requests.auth import HTTPBasicAuth
from pathlib import Path

# Configure logging
logger = logging.getLogger(__name__)

# Constants with proper path handling
BASE_DIR = "/opt/airflow/data"
LICENSES_FILE = os.path.join(BASE_DIR, "licenses.csv")
TRANSACTIONS_FILE = os.path.join(BASE_DIR, "transactions.csv")
DATABASE_FILE = os.path.join(BASE_DIR, "marketplace1.db")

# Create Datasets for dependency management
licenses_dataset = Dataset(LICENSES_FILE)
transactions_dataset = Dataset(TRANSACTIONS_FILE)

# SQL for creating the transactions table
CREATE_TRANSACTIONS_TABLE_SQL = """
-- First create the table
CREATE TABLE IF NOT EXISTS transactions (
    saleDate TIMESTAMP,
    transactionId TEXT PRIMARY KEY,
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
    purchasePrice REAL,
    vendorAmount REAL,
    partnerDiscountAmount REAL,
    paymentStatus TEXT,
    dunningStatus TEXT,
    refundReason TEXT,
    creditNoteReason TEXT,
    loyaltyDiscountAmount REAL,
    loyaltyDiscountReason TEXT,
    marketplacePromotionDiscountAmount REAL,
    marketplacePromotionDiscountCode TEXT,
    marketplacePromotionDiscountReason TEXT,
    expertDiscountAmount REAL,
    expertDiscountReason TEXT,
    manualDiscountAmount REAL,
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
    transactionLineItemId TEXT,
    appEdition TEXT
);
"""

# Separate SQL for creating indexes
CREATE_TRANSACTIONS_INDEXES_SQL = """
-- Then create the indexes after the table exists
CREATE INDEX IF NOT EXISTS idx_transactions_saleDate ON transactions(saleDate);
CREATE INDEX IF NOT EXISTS idx_transactions_addonKey ON transactions(addonKey);
CREATE INDEX IF NOT EXISTS idx_transactions_licenseType ON transactions(licenseType);
CREATE INDEX IF NOT EXISTS idx_transactions_company ON transactions(company);
CREATE INDEX IF NOT EXISTS idx_transactions_region ON transactions(region);
CREATE INDEX IF NOT EXISTS idx_transactions_addonLicenseId ON transactions(addonLicenseId);
"""

CREATE_LICENSES_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS licenses (
    addonLicenseId TEXT PRIMARY KEY,
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
    extendedServerSupport TEXT,
    licenseSourceType TEXT,
    transactionAccountId TEXT,
    appEdition TEXT,
    upgradeEvaluationEdition TEXT,
    upgradeEvaluationStartDate DATE,
    upgradeEvaluationEndDate DATE
);

-- Create indexes for commonly queried fields
CREATE INDEX IF NOT EXISTS idx_addon_key ON licenses(addonKey);
CREATE INDEX IF NOT EXISTS idx_status ON licenses(status);
CREATE INDEX IF NOT EXISTS idx_company ON licenses(company);
CREATE INDEX IF NOT EXISTS idx_maintenance_dates ON licenses(maintenanceStartDate, maintenanceEndDate);
CREATE INDEX IF NOT EXISTS idx_license_type ON licenses(licenseType);
CREATE INDEX IF NOT EXISTS idx_region ON licenses(region);
CREATE INDEX IF NOT EXISTS idx_cloud_id ON licenses(cloudId);
"""

class AtlassianAPIError(Exception):
    """Custom exception for Atlassian API related errors"""
    pass

class FileSystemError(Exception):
    """Custom exception for file system related errors"""
    pass

def get_api_credentials() -> tuple[str, str]:
    """Retrieve and validate API credentials from environment variables."""
    api_email = os.getenv('ATLASSIAN_API_EMAIL')
    api_token = os.getenv('ATLASSIAN_API_TOKEN')
    
    if not api_email or not api_token:
        raise ValueError(
            "Missing required credentials. Ensure ATLASSIAN_API_EMAIL and "
            "ATLASSIAN_API_TOKEN environment variables are set."
        )
    
    return api_email, api_token

def ensure_output_directory(directory: str) -> None:
    """Ensure the output directory exists with proper permissions."""
    try:
        Path(directory).mkdir(parents=True, exist_ok=True)
        current_user = os.getuid()
        current_group = os.getgid()
        
        os.chown(directory, current_user, current_group)
        os.chmod(directory, 0o775)
        
        logger.info(f"Directory {directory} setup complete with permissions 775")
    except Exception as e:
        logger.exception(f"Error setting up directory {directory}")
        raise FileSystemError(f"Failed to create/set permissions on directory: {str(e)}")

def download_from_api(
    vendor_id: str,
    auth: HTTPBasicAuth,
    data_type: Literal["licenses", "transactions"]
) -> requests.Response:
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
        raise AtlassianAPIError(f"Failed to download {data_type}: {str(e)}")

def save_response_to_file(response: requests.Response, file_path: str) -> None:
    """Save API response content to a file."""
    try:
        directory = os.path.dirname(file_path)
        ensure_output_directory(directory)
        
        with open(file_path, 'wb') as file:
            file.write(response.content)
        
        current_user = os.getuid()
        current_group = os.getgid()
        os.chown(file_path, current_user, current_group)
        os.chmod(file_path, 0o664)
        
        logger.info(f"Successfully saved data to {file_path} with permissions 664")
    except Exception as e:
        logger.exception(f"Failed to save file: {file_path}")
        raise FileSystemError(f"Failed to save file: {str(e)}")

@task(outlets=[licenses_dataset])
def download_licenses(vendor_id: Optional[str] = "1212980") -> None:
    """Download license file from Atlassian API."""
    try:
        api_email, api_token = get_api_credentials()
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
        api_email, api_token = get_api_credentials()
        auth = HTTPBasicAuth(api_email, api_token)
        response = download_from_api(vendor_id, auth, "transactions")
        save_response_to_file(response, TRANSACTIONS_FILE)
    except Exception as e:
        logger.exception("Error in download_transactions task")
        raise

@task
def initialize_database() -> None:
    """Initialize SQLite database and create tables."""
    try:
        ensure_output_directory(BASE_DIR)
        
        db_exists = os.path.exists(DATABASE_FILE)
        logger.info(f"{'Creating new' if not db_exists else 'Using existing'} database at {DATABASE_FILE}")
        
        with sqlite3.connect(DATABASE_FILE) as conn:
            # Set pragmas for better performance
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA busy_timeout=5000")
            
            cursor = conn.cursor()
            
            # Create transactions table
            logger.info("Creating transactions table...")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS transactions (
                    saleDate TIMESTAMP,
                    transactionId TEXT PRIMARY KEY,
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
                    purchasePrice REAL,
                    vendorAmount REAL,
                    partnerDiscountAmount REAL,
                    paymentStatus TEXT,
                    dunningStatus TEXT,
                    refundReason TEXT,
                    creditNoteReason TEXT,
                    loyaltyDiscountAmount REAL,
                    loyaltyDiscountReason TEXT,
                    marketplacePromotionDiscountAmount REAL,
                    marketplacePromotionDiscountCode TEXT,
                    marketplacePromotionDiscountReason TEXT,
                    expertDiscountAmount REAL,
                    expertDiscountReason TEXT,
                    manualDiscountAmount REAL,
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
                    transactionLineItemId TEXT,
                    appEdition TEXT
                )
            """)
            
            # Create licenses table
            logger.info("Creating licenses table...")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS licenses (
                    addonLicenseId TEXT PRIMARY KEY,
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
                    extendedServerSupport TEXT,
                    licenseSourceType TEXT,
                    transactionAccountId TEXT,
                    appEdition TEXT,
                    upgradeEvaluationEdition TEXT,
                    upgradeEvaluationStartDate DATE,
                    upgradeEvaluationEndDate DATE
                )
            """)
            
            # Create indexes one by one
            logger.info("Creating indexes...")
            
            # Transactions indexes
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_transactions_saleDate ON transactions(saleDate)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_transactions_addonKey ON transactions(addonKey)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_transactions_licenseType ON transactions(licenseType)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_transactions_company ON transactions(company)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_transactions_region ON transactions(region)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_transactions_addonLicenseId ON transactions(addonLicenseId)")
            
            # Licenses indexes
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_licenses_addonKey ON licenses(addonKey)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_licenses_status ON licenses(status)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_licenses_company ON licenses(company)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_licenses_region ON licenses(region)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_licenses_licenseType ON licenses(licenseType)")
            
            conn.commit()
            
            # Verify tables
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = cursor.fetchall()
            logger.info(f"Database tables: {[table[0] for table in tables]}")
            
            # Verify indexes
            cursor.execute("SELECT name FROM sqlite_master WHERE type='index'")
            indexes = cursor.fetchall()
            logger.info(f"Database indexes: {[index[0] for index in indexes]}")
            
        # Set proper permissions
        current_user = os.getuid()
        current_group = os.getgid()
        os.chown(DATABASE_FILE, current_user, current_group)
        os.chmod(DATABASE_FILE, 0o664)
        
        # Handle WAL files
        for ext in ['-wal', '-shm']:
            wal_file = f"{DATABASE_FILE}{ext}"
            if os.path.exists(wal_file):
                os.chown(wal_file, current_user, current_group)
                os.chmod(wal_file, 0o664)
        
        logger.info(f"Successfully initialized database at {DATABASE_FILE}")
        
    except sqlite3.Error as e:
        logger.exception("SQLite error while initializing database")
        raise
    except Exception as e:
        logger.exception("Error initializing database")
        raise

@task
def load_transactions() -> None:
    """Load transactions data from CSV to SQLite database."""
    try:
        if not os.path.exists(TRANSACTIONS_FILE):
            raise FileNotFoundError(f"Transactions file not found at {TRANSACTIONS_FILE}")
        
        logger.info("Reading transactions CSV file")
        df = pd.read_csv(TRANSACTIONS_FILE)
        
        # Log initial data shape
        logger.info(f"Initial data shape: {df.shape}")
        
        # Check and log missing values for each column
        missing_counts = df.isnull().sum()
        missing_percentages = (missing_counts / len(df)) * 100
        
        logger.info("Missing value analysis:")
        for col in df.columns:
            missing_pct = missing_percentages[col]
            logger.info(f"{col}: {missing_pct:.2f}% missing ({missing_counts[col]} rows)")
        
        # Convert empty strings to None/NULL
        df = df.replace({'': None, 'null': None, 'NULL': None})
        
        # Convert date/timestamp columns
        date_columns = [
            'saleDate',
            'maintenanceStartDate',
            'maintenanceEndDate',
            'originalTransactionSaleDate',
            'lastUpdated'
        ]
        
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
                null_dates = df[col].isnull().sum()
                logger.info(f"Converted {col} to datetime. {null_dates} null values found.")
        
        # Convert numeric columns
        numeric_columns = [
            'purchasePrice',
            'vendorAmount',
            'partnerDiscountAmount',
            'loyaltyDiscountAmount',
            'marketplacePromotionDiscountAmount',
            'expertDiscountAmount',
            'manualDiscountAmount'
        ]
        
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                null_amounts = df[col].isnull().sum()
                logger.info(f"Converted {col} to numeric. {null_amounts} null values found.")
        
        # Log value distributions for key columns
        key_columns = ['addonKey', 'licenseType', 'saleType', 'paymentStatus', 'region']
        logger.info("\nValue distribution for key columns:")
        for col in key_columns:
            if col in df.columns:
                value_counts = df[col].value_counts(dropna=False).head()
                logger.info(f"\n{col} top values:")
                for value, count in value_counts.items():
                    logger.info(f"  {value}: {count} ({count/len(df)*100:.2f}%)")
        
        # Log summary statistics for numeric columns
        logger.info("\nNumeric columns summary:")
        numeric_summary = df[numeric_columns].describe()
        logger.info(numeric_summary)
        
        logger.info("Loading data to SQLite database")
        with sqlite3.connect(DATABASE_FILE) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA busy_timeout=5000")
            
            # Load data, keeping original column names
            df.to_sql('transactions', conn, if_exists='replace', index=False)
        
        # Final data quality report
        logger.info("\nFinal Data Quality Report:")
        logger.info(f"Total transactions processed: {len(df)}")
        logger.info(f"Date range: {df['saleDate'].min()} to {df['saleDate'].max()}")
        logger.info(f"Total revenue: {df['vendorAmount'].sum():,.2f}")
        logger.info(f"Unique products: {df['addonKey'].nunique()}")
        logger.info(f"Unique companies: {df['company'].nunique()}")
        
        # Log sample of records for verification
        logger.info("\nSample of loaded data:")
        with sqlite3.connect(DATABASE_FILE) as conn:
            sample = pd.read_sql("SELECT * FROM transactions LIMIT 5", conn)
            logger.info(sample)
        
    except Exception as e:
        logger.exception("Error loading transactions data")
        raise

@task
def load_licenses() -> None:
    """Load licenses data from CSV to SQLite database."""
    try:
        if not os.path.exists(LICENSES_FILE):
            raise FileNotFoundError(f"Licenses file not found at {LICENSES_FILE}")
        
        logger.info("Reading licenses CSV file")
        df = pd.read_csv(LICENSES_FILE)
        
        # Log initial data shape
        logger.info(f"Initial data shape: {df.shape}")
        
        # Check and log missing values for each column
        missing_counts = df.isnull().sum()
        missing_percentages = (missing_counts / len(df)) * 100
        
        logger.info("Missing value analysis:")
        for col in df.columns:
            missing_pct = missing_percentages[col]
            logger.info(f"{col}: {missing_pct:.2f}% missing ({missing_counts[col]} rows)")
        
        # Convert empty strings to None/NULL
        df = df.replace({'': None, 'null': None, 'NULL': None})
        
        # Convert date columns, handling null values
        date_columns = [
            'maintenanceStartDate',
            'maintenanceEndDate',
            'lastUpdated',
            'upgradeEvaluationStartDate',
            'upgradeEvaluationEndDate'
        ]
        
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')  # 'coerce' will set invalid dates to NaT
                null_dates = df[col].isnull().sum()
                logger.info(f"Converted {col} to datetime. {null_dates} null values found.")
        
        # Handle boolean columns
        boolean_columns = ['extendedServerSupport']
        for col in boolean_columns:
            if col in df.columns:
                df[col] = df[col].map({'true': 1, 'false': 0, True: 1, False: 0, None: None})
        
        # Log non-empty value counts for key columns
        key_columns = ['addonKey', 'licenseType', 'status', 'company', 'region']
        logger.info("\nValue distribution for key columns:")
        for col in key_columns:
            if col in df.columns:
                value_counts = df[col].value_counts(dropna=False).head()
                logger.info(f"\n{col} top values:")
                for value, count in value_counts.items():
                    logger.info(f"  {value}: {count} ({count/len(df)*100:.2f}%)")
        
        logger.info("Loading data to SQLite database")
        with sqlite3.connect(DATABASE_FILE) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA busy_timeout=5000")
            
            # Load data, keeping original column names
            df.to_sql('licenses', conn, if_exists='replace', index=False)
        
        # Final data quality report
        logger.info("\nFinal Data Quality Report:")
        logger.info(f"Total rows processed: {len(df)}")
        logger.info(f"Columns with data: {len([col for col in df.columns if df[col].notna().any()])}")
        logger.info(f"Empty columns: {len([col for col in df.columns if df[col].isna().all()])}")
        
        # Log sample of records for verification
        logger.info("\nSample of loaded data:")
        with sqlite3.connect(DATABASE_FILE) as conn:
            sample = pd.read_sql("SELECT * FROM licenses LIMIT 5", conn)
            logger.info(sample)
        
    except Exception as e:
        logger.exception("Error loading licenses data")
        raise


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
    description='Downloads Atlassian Marketplace data and loads to SQLite',
    schedule='@daily',
    catchup=False,
    tags=['atlassian', 'marketplace', 'etl'],
) as dag:
    
    # Download tasks
    licenses_task = download_licenses()
    transactions_task = download_transactions()
    
    # Database tasks
    init_db = initialize_database()
    load_licenses_data = load_licenses()
    load_transactions_data = load_transactions()
    
    # Set task dependencies
    [licenses_task, transactions_task] >> init_db
    init_db >> load_licenses_data
    init_db >> load_transactions_data
