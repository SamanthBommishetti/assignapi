import pandas as pd
# import numpy as np
from clickhouse_connect import get_client
from clickhouse_connect.driver.exceptions import OperationalError
from azure.storage.blob import BlobServiceClient
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def azure_read_file():
    # Replace with your storage account connection string
    connect_str = os.getenv("FMAC_STORAGE_CONNECTION_STRING") 
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_name="freddiemac"
    container_client = blob_service_client.get_container_client(container_name)
    # blob_list = container_client.list_blobs()
    # for blob in blob_list:
    #     print(blob.name)

    blob_name="standard/extracted_files/historical_data_1999Q2/historical_data_time_1999Q2.txt"
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    download_stream = blob_client.download_blob()
    records_read = 0
    max_records_to_read = 1
    processed_data = []

    # Assuming records are line-separated
    for line in download_stream.chunks():
        # Decode the line if it's text data
        decoded_line = line.decode('utf-8').strip()

        # Process the record
        processed_data.append(decoded_line)
        records_read += 1

        if records_read >= max_records_to_read:
            break

    print(f"Read {records_read} records.")
    # print(processed_data)
    # Further processing of processed_data

#Create Origination table if not exits
def create_origination_table(client):
    """Create the origination table in ClickHouse if it doesn't exist."""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS fmac_db.origination (
        credit_score Nullable(Int32),
        first_payment_date Nullable(Date),
        first_time_homebuyer_flag Nullable(String),
        maturity_date Nullable(Date),
        msa_or_metro_division Nullable(Int32),
        mi_pct Nullable(Float64),
        number_of_units Nullable(Int32),
        occupancy_status Nullable(String),
        original_cltv Nullable(Int32),
        original_dti_ratio Nullable(Int32),
        original_upb Nullable(Float64),
        original_ltv Nullable(Int32),
        original_interest_rate Nullable(Float64),
        channel Nullable(String),
        ppm_flag Nullable(String),
        amortization_type Nullable(String),
        property_state Nullable(String),
        property_type Nullable(String),
        postal_code Nullable(UInt32),
        loan_sequence_number Nullable(String),
        loan_purpose Nullable(String),
        original_loan_term Nullable(Int32),
        number_of_borrowers Nullable(Int32),
        seller_name Nullable(String),
        servicer_name Nullable(String),
        super_conforming_flag Nullable(String),
        preharp_loan_sequence_number Nullable(String),
        program_indicator Nullable(String),
        harp_indicator Nullable(String),
        property_valuation_method Nullable(Int32),
        interest_only_indicator Nullable(String),
        mi_cancellation_indicator Nullable(String)
    ) ENGINE = MergeTree()
    ORDER BY (loan_sequence_number)
    """
    try:
        client.command(create_table_query)
        print("✅ Table 'origination' created or already exists in fmac_db.")
    except OperationalError as e:
        print(f"❌ Failed to create table: {e}")
        raise

#Create performance table, if it not exists. 
def create_performance_table(client):
    """Create the origination table in ClickHouse if it doesn't exist."""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS performance (
        loan_sequence_number String,
        monthly_reporting_period Date,
        current_actual_upb Nullable(Float64),
        current_loan_delinquency_status Nullable(String),
        loan_age Nullable(Int32),
        remaining_months_to_legal_maturity Nullable(Int32),
        defect_settlement_date Nullable(String),
        modification_flag Nullable(String),
        zero_balance_code Nullable(String),
        zero_balance_effective_date Nullable(String),
        current_interest_rate Nullable(Float32),
        current_deferred_upb Nullable(Float64),
        ddlpi Nullable(String),
        mi_recoveries Nullable(Float64),
        net_sale_proceeds Nullable(String),
        non_mi_recoveries Nullable(Float64),
        expenses Nullable(Float64),
        legal_costs Nullable(Float64),
        maintenance_preservation_costs Nullable(Float64),
        taxes_insurance Nullable(Float64),
        misc_expenses Nullable(Float64),
        actual_loss Nullable(Float64),
        modification_cost Nullable(Float64),
        step_modification_flag Nullable(String),
        deferred_payment_plan Nullable(String),
        estimated_ltv Nullable(Int32),
        zero_balance_removal_upb Nullable(Float64),
        delinquent_accrued_interest Nullable(Float64),
        delinquency_due_to_disaster Nullable(String),
        borrower_assistance_status_code Nullable(String),
        current_month_modification_cost Nullable(Float64),
        interest_bearing_upb Nullable(Float64)
    )
    ENGINE = MergeTree()
    PARTITION BY toYYYYMM(monthly_reporting_period)
    ORDER BY (monthly_reporting_period, loan_sequence_number)
    """
    try:
        client.command(create_table_query)
        print("✅ Table 'performance' created or already exists in fmac_db.")
    except OperationalError as e:
        print(f"❌ Failed to create table: {e}")
        raise

def azure_to_clickhouse_performance():
    # Replace with your storage account connection string
    connect_str = os.getenv("FMAC_STORAGE_CONNECTION_STRING") 
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_name="freddiemac"
    container_client = blob_service_client.get_container_client(container_name)
    # blob_list = container_client.list_blobs()
    # for blob in blob_list:
    #     print(blob.name)
    blob_name="standard/extracted_files/historical_data_1999Q2/historical_data_time_1999Q2.txt"
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    download_stream = blob_client.download_blob()
    records_read = 0
    max_records_to_read = 1
    processed_data = []

    # Assuming records are line-separated
    for line in download_stream.chunks():
        # Decode the line if it's text data
        decoded_line = line.decode('utf-8').strip()

        # Process the record
        processed_data.append(decoded_line)
        records_read += 1

        if records_read >= max_records_to_read:
            break

    print(f"Read {records_read} records.")
    # print(processed_data)
    # Further processing of processed_data


    # Step 1: Connect to ClickHouse
    # --- Config ---
    clickhouse_host = "57.159.27.80"
    clickhouse_port = 8123
    clickhouse_database = "fmac_db"
    clickhouse_user = "fmacadmin"
    clickhouse_password = "fmac*2025"
 
    print("🔌 Attempting to connect to ClickHouse...")
    try:
        client = get_client(
            host=clickhouse_host,
            port=clickhouse_port,
            username=clickhouse_user,
            password=clickhouse_password,
            database=clickhouse_database
        )
        version = client.command("SELECT version()")
        print(f"✅ Connected to ClickHouse. Server version: {version}")
        # Verify database exists
        databases = client.query("SHOW DATABASES").result_rows
        if not any(db[0] == clickhouse_database for db in databases):
            print(f"❌ Database '{clickhouse_database}' does not exist.")
            print("Create it with: CREATE DATABASE IF NOT EXISTS fmac_db;")
            return
    except OperationalError as e:
        print(f"❌ Connection failed: {e}")
        print("Troubleshooting steps:")
        print(f"1. Verify server is running at {clickhouse_host}:{clickhouse_port}.")
        print(f"2. Check credentials: user={clickhouse_user}, password=<hidden>.")
        print("3. Ensure port 8123 is open (use 'telnet 57.159.27.80 8123' or check firewall/security groups).")
        print("4. Test connection with curl: 'curl http://57.159.27.80:8123'.")
        return
    except Exception as e:
        print(f"❌ Unexpected error during connection: {e}")
        return
 
    # Step 2: Create table if it doesn't exist
    create_origination_table(client)
 
    # Step 3: Load the TXT file
    print("📂 Loading TXT file...")
    try:
        df = pd.read_csv("temp_file.txt", delimiter="|", engine="python", header=None)
    except FileNotFoundError:
        print("❌ Error: 'temp_file.txt' not found in the current directory.")
        return
    except Exception as e:
        print(f"❌ Error loading file: {e}")
        return
 
    # Step 4: Assign column names
    expected_columns = [
        "credit_score", "first_payment_date", "first_time_homebuyer_flag", "maturity_date",
        "msa_or_metro_division", "mi_pct", "number_of_units", "occupancy_status",
        "original_cltv", "original_dti_ratio", "original_upb", "original_ltv",
        "original_interest_rate", "channel", "ppm_flag", "amortization_type",
        "property_state", "property_type", "postal_code", "loan_sequence_number",
        "loan_purpose", "original_loan_term", "number_of_borrowers", "seller_name",
        "servicer_name", "super_conforming_flag", "preharp_loan_sequence_number",
        "program_indicator", "harp_indicator", "property_valuation_method",
        "interest_only_indicator", "mi_cancellation_indicator"
    ]
 
    if len(df.columns) != len(expected_columns):
        print(f"❌ Error: File has {len(df.columns)} columns, expected {len(expected_columns)}.")
        return
    df.columns = expected_columns
    print(f"📊 Loaded {len(df)} rows, {len(df.columns)} columns")
 
    # Step 5: Fill null values and clean data
    print("🧹 Cleaning data and filling null values...")
    integer_cols = [
        "credit_score", "msa_or_metro_division", "number_of_units", "original_cltv",
        "original_dti_ratio", "original_ltv", "postal_code", "original_loan_term",
        "number_of_borrowers", "property_valuation_method"
    ]
    double_cols = ["mi_pct", "original_upb", "original_interest_rate"]
    string_cols = [
        "first_time_homebuyer_flag", "occupancy_status", "channel", "ppm_flag",
        "amortization_type", "property_state", "property_type", "loan_sequence_number",
        "loan_purpose", "seller_name", "servicer_name", "super_conforming_flag",
        "preharp_loan_sequence_number", "program_indicator", "harp_indicator",
        "interest_only_indicator", "mi_cancellation_indicator"
    ]
    date_cols = ["first_payment_date", "maturity_date"]
 
    try:
        # Fill nulls for integer columns and convert to Python int
        df[integer_cols] = df[integer_cols].apply(pd.to_numeric, errors="coerce").fillna(0).astype("Int32")
        # Fill nulls for double columns and convert to Python float
        df[double_cols] = df[double_cols].apply(pd.to_numeric, errors="coerce").fillna(0.0).astype(float)
        # Ensure string columns are strings, handle any numeric values
        for col in string_cols:
            df[col] = df[col].astype(str).replace("nan", "")
        # Convert date columns to string format (YYYY-MM-DD) or None
        for col in date_cols:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d")
            df[col] = df[col].where(df[col].notnull(), None)
    except Exception as e:
        print(f"❌ Error cleaning data: {e}")
        return
 
    # Debug: Print column types and sample data
    print("🔍 DataFrame dtypes before insertion:")
    print(df.dtypes)
    print("🔍 Sample data (first 2 rows):")
    print(df.head(2))
 
    # Step 6: Insert into ClickHouse
    print("⬆️ Inserting into ClickHouse table 'origination'...")
    try:
        client.insert_df("fmac_db.origination", df)
        print("✅ Data appended successfully!")
    except OperationalError as e:
        print(f"❌ Error inserting data: {e}")
        print("Verify table schema matches DataFrame columns and data types.")
        return
    except Exception as e:
        print(f"❌ Unexpected error during insertion: {e}")
        return
    
def write_to_clickhouse_From_local_text_File():
    # Step 1: Connect to ClickHouse
    # --- Config ---
    clickhouse_host = "57.159.27.80"
    clickhouse_port = 8123
    clickhouse_database = "fmac_db"
    clickhouse_user = "fmacadmin"
    clickhouse_password = "fmac*2025"
 
    print("🔌 Attempting to connect to ClickHouse...")
    try:
        client = get_client(
            host=clickhouse_host,
            port=clickhouse_port,
            username=clickhouse_user,
            password=clickhouse_password,
            database=clickhouse_database
        )
        version = client.command("SELECT version()")
        print(f"✅ Connected to ClickHouse. Server version: {version}")
        # Verify database exists
        databases = client.query("SHOW DATABASES").result_rows
        if not any(db[0] == clickhouse_database for db in databases):
            print(f"❌ Database '{clickhouse_database}' does not exist.")
            print("Create it with: CREATE DATABASE IF NOT EXISTS fmac_db;")
            return
    except OperationalError as e:
        print(f"❌ Connection failed: {e}")
        print("Troubleshooting steps:")
        print(f"1. Verify server is running at {clickhouse_host}:{clickhouse_port}.")
        print(f"2. Check credentials: user={clickhouse_user}, password=<hidden>.")
        print("3. Ensure port 8123 is open (use 'telnet 57.159.27.80 8123' or check firewall/security groups).")
        print("4. Test connection with curl: 'curl http://57.159.27.80:8123'.")
        return
    except Exception as e:
        print(f"❌ Unexpected error during connection: {e}")
        return
 
    # Step 2: Create table if it doesn't exist
    create_origination_table(client)
 
    # Step 3: Load the TXT file
    print("📂 Loading TXT file...")
    try:
        df = pd.read_csv("temp_file.txt", delimiter="|", engine="python", header=None)
    except FileNotFoundError:
        print("❌ Error: 'temp_file.txt' not found in the current directory.")
        return
    except Exception as e:
        print(f"❌ Error loading file: {e}")
        return
 
    # Step 4: Assign column names
    expected_columns = [
        "credit_score", "first_payment_date", "first_time_homebuyer_flag", "maturity_date",
        "msa_or_metro_division", "mi_pct", "number_of_units", "occupancy_status",
        "original_cltv", "original_dti_ratio", "original_upb", "original_ltv",
        "original_interest_rate", "channel", "ppm_flag", "amortization_type",
        "property_state", "property_type", "postal_code", "loan_sequence_number",
        "loan_purpose", "original_loan_term", "number_of_borrowers", "seller_name",
        "servicer_name", "super_conforming_flag", "preharp_loan_sequence_number",
        "program_indicator", "harp_indicator", "property_valuation_method",
        "interest_only_indicator", "mi_cancellation_indicator"
    ]
 
    if len(df.columns) != len(expected_columns):
        print(f"❌ Error: File has {len(df.columns)} columns, expected {len(expected_columns)}.")
        return
    df.columns = expected_columns
    print(f"📊 Loaded {len(df)} rows, {len(df.columns)} columns")
 
    # Step 5: Fill null values and clean data
    print("🧹 Cleaning data and filling null values...")
    integer_cols = [
        "credit_score", "msa_or_metro_division", "number_of_units", "original_cltv",
        "original_dti_ratio", "original_ltv", "postal_code", "original_loan_term",
        "number_of_borrowers", "property_valuation_method"
    ]
    double_cols = ["mi_pct", "original_upb", "original_interest_rate"]
    string_cols = [
        "first_time_homebuyer_flag", "occupancy_status", "channel", "ppm_flag",
        "amortization_type", "property_state", "property_type", "loan_sequence_number",
        "loan_purpose", "seller_name", "servicer_name", "super_conforming_flag",
        "preharp_loan_sequence_number", "program_indicator", "harp_indicator",
        "interest_only_indicator", "mi_cancellation_indicator"
    ]
    date_cols = ["first_payment_date", "maturity_date"]
 
    try:
        # Fill nulls for integer columns and convert to Python int
        df[integer_cols] = df[integer_cols].apply(pd.to_numeric, errors="coerce").fillna(0).astype("Int32")
        # Fill nulls for double columns and convert to Python float
        df[double_cols] = df[double_cols].apply(pd.to_numeric, errors="coerce").fillna(0.0).astype(float)
        # Ensure string columns are strings, handle any numeric values
        for col in string_cols:
            df[col] = df[col].astype(str).replace("nan", "")
        # Convert date columns to string format (YYYY-MM-DD) or None
        for col in date_cols:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d")
            df[col] = df[col].where(df[col].notnull(), None)
    except Exception as e:
        print(f"❌ Error cleaning data: {e}")
        return
 
    # Debug: Print column types and sample data
    print("🔍 DataFrame dtypes before insertion:")
    print(df.dtypes)
    print("🔍 Sample data (first 2 rows):")
    print(df.head(2))
 
    # Step 6: Insert into ClickHouse
    print("⬆️ Inserting into ClickHouse table 'origination'...")
    try:
        client.insert_df("fmac_db.origination", df)
        print("✅ Data appended successfully!")
    except OperationalError as e:
        print(f"❌ Error inserting data: {e}")
        print("Verify table schema matches DataFrame columns and data types.")
        return
    except Exception as e:
        print(f"❌ Unexpected error during insertion: {e}")
        return

 
if __name__ == "__main__":
    azure_read_file()

 
