import pandas as pd
import os
import random
from datetime import datetime
from sqlalchemy.orm import sessionmaker
from app.core.database import engine, default_session
from app.general.models import GeneralQuery

# Define paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
destination_folder = "../general_out_csvfiles"
mount_path = "general_out_csvfiles"
OUTCSV_DIR = os.path.join(BASE_DIR, destination_folder)

# Ensure output directory exists
os.makedirs(OUTCSV_DIR, exist_ok=True)

# Define realistic data pools
STATES = ['CA', 'TX', 'FL', 'NY', 'IL', 'PA', 'OH', 'GA', 'NC', 'MI']
YEARS = list(range(1999, 2026))
MONTHS = list(range(1, 13))
LOAN_PURPOSES = ['P', 'C', 'N']
AMORTIZATION_TYPES = ['FRM', 'ARM']
ZERO_BALANCE_CODES = ['01', '02', '03', '06', '09']

# Dummy SQL queries for each type (20 each, totaling 80)
area_wise_queries = [
    "SELECT property_state, COUNT(*) as loan_count FROM origination_table GROUP BY property_state ORDER BY loan_count DESC;",
    "SELECT property_state, AVG(original_upb) as avg_loan_amount FROM origination_table GROUP BY property_state;",
    "SELECT property_state, AVG(original_interest_rate) as avg_interest_rate FROM origination_table GROUP BY property_state;",
    "SELECT property_state, COUNT(CASE WHEN original_ltv > 80 THEN 1 END) as high_ltv_loans FROM origination_table GROUP BY property_state;",
    "SELECT property_state, SUM(original_upb) as total_upb FROM origination_table GROUP BY property_state;",
    "SELECT property_state, AVG(credit_score) as avg_credit_score FROM origination_table GROUP BY property_state;",
    "SELECT property_state, COUNT(CASE WHEN loan_purpose = 'P' THEN 1 END) as purchase_loans FROM origination_table GROUP BY property_state;",
    "SELECT property_state, COUNT(CASE WHEN first_time_homebuyer_flag = 'Y' THEN 1 END) as first_time_buyers FROM origination_table GROUP BY property_state;",
    "SELECT property_state, COUNT(DISTINCT seller_name) as unique_sellers FROM origination_table GROUP BY property_state;",
    "SELECT property_state, AVG(original_dti_ratio) as avg_dti FROM origination_table GROUP BY property_state;",
    "SELECT property_state, COUNT(CASE WHEN property_type = 'SF' THEN 1 END) as single_family FROM origination_table GROUP BY property_state;",
    "SELECT property_state, SUM(mi_recoveries) as total_mi_recoveries FROM performance_table GROUP BY property_state;",
    "SELECT property_state, AVG(current_actual_upb) as avg_current_upb FROM performance_table GROUP BY property_state;",
    "SELECT property_state, COUNT(CASE WHEN current_loan_delinquency_status != '0' THEN 1 END) as delinquent_loans FROM performance_table GROUP BY property_state;",
    "SELECT property_state, SUM(actual_loss) as total_losses FROM performance_table GROUP BY property_state;",
    "SELECT property_state, AVG(loan_age) as avg_loan_age FROM performance_table GROUP BY property_state;",
    "SELECT property_state, COUNT(CASE WHEN zero_balance_code = '01' THEN 1 END) as paid_off_loans FROM performance_table GROUP BY property_state;",
    "SELECT property_state, AVG(modification_cost) as avg_mod_cost FROM performance_table GROUP BY property_state;",
    "SELECT property_state, COUNT(CASE WHEN modification_flag = 'Y' THEN 1 END) as modified_loans FROM performance_table GROUP BY property_state;",
    "SELECT property_state, SUM(expenses) as total_expenses FROM performance_table GROUP BY property_state;"
]

year_wise_queries = [
    "SELECT first_payment_year, COUNT(*) as loan_count FROM origination_table GROUP BY first_payment_year ORDER BY first_payment_year;",
    "SELECT first_payment_year, AVG(original_upb) as avg_loan_amount FROM origination_table GROUP BY first_payment_year;",
    "SELECT first_payment_year, AVG(original_interest_rate) as avg_interest_rate FROM origination_table GROUP BY first_payment_year;",
    "SELECT first_payment_year, COUNT(CASE WHEN original_ltv > 80 THEN 1 END) as high_ltv_loans FROM origination_table GROUP BY first_payment_year;",
    "SELECT first_payment_year, SUM(original_upb) as total_upb FROM origination_table GROUP BY first_payment_year;",
    "SELECT first_payment_year, AVG(credit_score) as avg_credit_score FROM origination_table GROUP BY first_payment_year;",
    "SELECT first_payment_year, COUNT(CASE WHEN loan_purpose = 'P' THEN 1 END) as purchase_loans FROM origination_table GROUP BY first_payment_year;",
    "SELECT first_payment_year, COUNT(CASE WHEN first_time_homebuyer_flag = 'Y' THEN 1 END) as first_time_buyers FROM origination_table GROUP BY first_payment_year;",
    "SELECT first_payment_year, COUNT(DISTINCT seller_name) as unique_sellers FROM origination_table GROUP BY first_payment_year;",
    "SELECT first_payment_year, AVG(original_dti_ratio) as avg_dti FROM origination_table GROUP BY first_payment_year;",
    "SELECT first_payment_year, COUNT(CASE WHEN property_type = 'SF' THEN 1 END) as single_family FROM origination_table GROUP BY first_payment_year;",
    "SELECT year, SUM(mi_recoveries) as total_mi_recoveries FROM performance_table GROUP BY year;",
    "SELECT year, AVG(current_actual_upb) as avg_current_upb FROM performance_table GROUP BY year;",
    "SELECT year, COUNT(CASE WHEN current_loan_delinquency_status != '0' THEN 1 END) as delinquent_loans FROM performance_table GROUP BY year;",
    "SELECT year, SUM(actual_loss) as total_losses FROM performance_table GROUP BY year;",
    "SELECT year, AVG(loan_age) as avg_loan_age FROM performance_table GROUP BY year;",
    "SELECT year, COUNT(CASE WHEN zero_balance_code = '01' THEN 1 END) as paid_off_loans FROM performance_table GROUP BY year;",
    "SELECT year, AVG(modification_cost) as avg_mod_cost FROM performance_table GROUP BY year;",
    "SELECT year, COUNT(CASE WHEN modification_flag = 'Y' THEN 1 END) as modified_loans FROM performance_table GROUP BY year;",
    "SELECT year, SUM(expenses) as total_expenses FROM performance_table GROUP BY year;"
]

month_wise_queries = [
    "SELECT first_payment_month, COUNT(*) as loan_count FROM origination_table GROUP BY first_payment_month ORDER BY first_payment_month;",
    "SELECT first_payment_month, AVG(original_upb) as avg_loan_amount FROM origination_table GROUP BY first_payment_month;",
    "SELECT first_payment_month, AVG(original_interest_rate) as avg_interest_rate FROM origination_table GROUP BY first_payment_month;",
    "SELECT first_payment_month, COUNT(CASE WHEN original_ltv > 80 THEN 1 END) as high_ltv_loans FROM origination_table GROUP BY first_payment_month;",
    "SELECT first_payment_month, SUM(original_upb) as total_upb FROM origination_table GROUP BY first_payment_month;",
    "SELECT first_payment_month, AVG(credit_score) as avg_credit_score FROM origination_table GROUP BY first_payment_month;",
    "SELECT first_payment_month, COUNT(CASE WHEN loan_purpose = 'P' THEN 1 END) as purchase_loans FROM origination_table GROUP BY first_payment_month;",
    "SELECT first_payment_month, COUNT(CASE WHEN first_time_homebuyer_flag = 'Y' THEN 1 END) as first_time_buyers FROM origination_table GROUP BY first_payment_month;",
    "SELECT first_payment_month, COUNT(DISTINCT seller_name) as unique_sellers FROM origination_table GROUP BY first_payment_month;",
    "SELECT first_payment_month, AVG(original_dti_ratio) as avg_dti FROM origination_table GROUP BY first_payment_month;",
    "SELECT first_payment_month, COUNT(CASE WHEN property_type = 'SF' THEN 1 END) as single_family FROM origination_table GROUP BY first_payment_month;",
    "SELECT month, SUM(mi_recoveries) as total_mi_recoveries FROM performance_table GROUP BY month;",
    "SELECT month, AVG(current_actual_upb) as avg_current_upb FROM performance_table GROUP BY month;",
    "SELECT month, COUNT(CASE WHEN current_loan_delinquency_status != '0' THEN 1 END) as delinquent_loans FROM performance_table GROUP BY month;",
    "SELECT month, SUM(actual_loss) as total_losses FROM performance_table GROUP BY month;",
    "SELECT month, AVG(loan_age) as avg_loan_age FROM performance_table GROUP BY month;",
    "SELECT month, COUNT(CASE WHEN zero_balance_code = '01' THEN 1 END) as paid_off_loans FROM performance_table GROUP BY month;",
    "SELECT month, AVG(modification_cost) as avg_mod_cost FROM performance_table GROUP BY month;",
    "SELECT month, COUNT(CASE WHEN modification_flag = 'Y' THEN 1 END) as modified_loans FROM performance_table GROUP BY month;",
    "SELECT month, SUM(expenses) as total_expenses FROM performance_table GROUP BY month;"
]

loan_type_queries = [
    "SELECT loan_purpose, COUNT(*) as loan_count FROM origination_table GROUP BY loan_purpose;",
    "SELECT loan_purpose, AVG(original_upb) as avg_loan_amount FROM origination_table GROUP BY loan_purpose;",
    "SELECT loan_purpose, AVG(original_interest_rate) as avg_interest_rate FROM origination_table GROUP BY loan_purpose;",
    "SELECT loan_purpose, COUNT(CASE WHEN original_ltv > 80 THEN 1 END) as high_ltv_loans FROM origination_table GROUP BY loan_purpose;",
    "SELECT loan_purpose, SUM(original_upb) as total_upb FROM origination_table GROUP BY loan_purpose;",
    "SELECT loan_purpose, AVG(credit_score) as avg_credit_score FROM origination_table GROUP BY loan_purpose;",
    "SELECT loan_purpose, AVG(original_dti_ratio) as avg_dti FROM origination_table GROUP BY loan_purpose;",
    "SELECT loan_purpose, COUNT(CASE WHEN first_time_homebuyer_flag = 'Y' THEN 1 END) as first_time_buyers FROM origination_table GROUP BY loan_purpose;",
    "SELECT loan_purpose, COUNT(DISTINCT seller_name) as unique_sellers FROM origination_table GROUP BY loan_purpose;",
    "SELECT loan_purpose, COUNT(CASE WHEN property_type = 'SF' THEN 1 END) as single_family FROM origination_table GROUP BY loan_purpose;",
    "SELECT amortization_type, COUNT(*) as loan_count FROM origination_table GROUP BY amortization_type;",
    "SELECT amortization_type, AVG(original_upb) as avg_loan_amount FROM origination_table GROUP BY amortization_type;",
    "SELECT amortization_type, AVG(original_interest_rate) as avg_interest_rate FROM origination_table GROUP BY amortization_type;",
    "SELECT amortization_type, SUM(original_upb) as total_upb FROM origination_table GROUP BY amortization_type;",
    "SELECT amortization_type, AVG(credit_score) as avg_credit_score FROM origination_table GROUP BY amortization_type;",
    "SELECT zero_balance_code, COUNT(*) as resolution_count FROM performance_table GROUP BY zero_balance_code;",
    "SELECT zero_balance_code, SUM(actual_loss) as total_losses FROM performance_table GROUP BY zero_balance_code;",
    "SELECT zero_balance_code, AVG(loan_age) as avg_loan_age FROM performance_table GROUP BY zero_balance_code;",
    "SELECT zero_balance_code, COUNT(CASE WHEN modification_flag = 'Y' THEN 1 END) as modified_loans FROM performance_table GROUP BY zero_balance_code;",
    "SELECT zero_balance_code, SUM(expenses) as total_expenses FROM performance_table GROUP BY zero_balance_code;"
]

# Combined for 80 queries
all_general_queries = area_wise_queries + year_wise_queries + month_wise_queries + loan_type_queries
query_types = ['area_wise'] * 20 + ['year_wise'] * 20 + ['month_wise'] * 20 + ['loan_type'] * 20

def generate_dummy_csv(query_type, query_sql, query_id):
    """Generate a realistic dummy CSV file based on query type and SQL."""
    # Extract the result column name from the SQL query
    result_column = query_sql.split(' as ')[-1].split(' ')[0]
    
    # Define columns and sample data based on query type and SQL
    if query_type == 'area_wise':
        group_by = 'property_state'
        data = [
            {
                group_by: state,
                result_column: (
                    random.randint(1000, 50000) if 'count' in result_column.lower() or 'buyers' in result_column.lower() or 'sellers' in result_column.lower() else
                    round(random.uniform(3.0, 7.0), 2) if 'interest_rate' in result_column.lower() else
                    random.randint(600, 850) if 'credit_score' in result_column.lower() else
                    random.randint(20, 50) if 'dti' in result_column.lower() or 'ltv' in result_column.lower() else
                    round(random.uniform(100000, 1000000), 2) if 'upb' in result_column.lower() or 'loss' in result_column.lower() or 'expenses' in result_column.lower() or 'recoveries' in result_column.lower() else
                    random.randint(12, 360) if 'loan_age' in result_column.lower() else
                    round(random.uniform(5000, 50000), 2) if 'cost' in result_column.lower() else
                    random.randint(100, 10000)
                )
            }
            for state in random.sample(STATES, k=random.randint(5, len(STATES)))
        ]
    elif query_type == 'year_wise':
        group_by = 'first_payment_year' if 'origination_table' in query_sql else 'year'
        data = [
            {
                group_by: year,
                result_column: (
                    random.randint(1000, 50000) if 'count' in result_column.lower() or 'buyers' in result_column.lower() or 'sellers' in result_column.lower() else
                    round(random.uniform(3.0, 7.0), 2) if 'interest_rate' in result_column.lower() else
                    random.randint(600, 850) if 'credit_score' in result_column.lower() else
                    random.randint(20, 50) if 'dti' in result_column.lower() or 'ltv' in result_column.lower() else
                    round(random.uniform(100000, 1000000), 2) if 'upb' in result_column.lower() or 'loss' in result_column.lower() or 'expenses' in result_column.lower() or 'recoveries' in result_column.lower() else
                    random.randint(12, 360) if 'loan_age' in result_column.lower() else
                    round(random.uniform(5000, 50000), 2) if 'cost' in result_column.lower() else
                    random.randint(100, 10000)
                )
            }
            for year in random.sample(YEARS, k=random.randint(5, 10))
        ]
    elif query_type == 'month_wise':
        group_by = 'first_payment_month' if 'origination_table' in query_sql else 'month'
        data = [
            {
                group_by: month,
                result_column: (
                    random.randint(1000, 50000) if 'count' in result_column.lower() or 'buyers' in result_column.lower() or 'sellers' in result_column.lower() else
                    round(random.uniform(3.0, 7.0), 2) if 'interest_rate' in result_column.lower() else
                    random.randint(600, 850) if 'credit_score' in result_column.lower() else
                    random.randint(20, 50) if 'dti' in result_column.lower() or 'ltv' in result_column.lower() else
                    round(random.uniform(100000, 1000000), 2) if 'upb' in result_column.lower() or 'loss' in result_column.lower() or 'expenses' in result_column.lower() or 'recoveries' in result_column.lower() else
                    random.randint(12, 360) if 'loan_age' in result_column.lower() else
                    round(random.uniform(5000, 50000), 2) if 'cost' in result_column.lower() else
                    random.randint(100, 10000)
                )
            }
            for month in random.sample(MONTHS, k=random.randint(6, 12))
        ]
    elif query_type == 'loan_type':
        group_by = (
            'loan_purpose' if 'loan_purpose' in query_sql else
            'amortization_type' if 'amortization_type' in query_sql else
            'zero_balance_code'
        )
        values = (
            LOAN_PURPOSES if 'loan_purpose' in query_sql else
            AMORTIZATION_TYPES if 'amortization_type' in query_sql else
            ZERO_BALANCE_CODES
        )
        data = [
            {
                group_by: val,
                result_column: (
                    random.randint(1000, 50000) if 'count' in result_column.lower() or 'buyers' in result_column.lower() or 'sellers' in result_column.lower() else
                    round(random.uniform(3.0, 7.0), 2) if 'interest_rate' in result_column.lower() else
                    random.randint(600, 850) if 'credit_score' in result_column.lower() else
                    random.randint(20, 50) if 'dti' in result_column.lower() or 'ltv' in result_column.lower() else
                    round(random.uniform(100000, 1000000), 2) if 'upb' in result_column.lower() or 'loss' in result_column.lower() or 'expenses' in result_column.lower() or 'recoveries' in result_column.lower() else
                    random.randint(12, 360) if 'loan_age' in result_column.lower() else
                    round(random.uniform(5000, 50000), 2) if 'cost' in result_column.lower() else
                    random.randint(100, 10000)
                )
            }
            for val in random.sample(values, k=len(values))
        ]
    
    # Create DataFrame and save to CSV
    df = pd.DataFrame(data)
    csv_filename = f"general_query_{query_id}_{query_type}.csv"
    csv_file_path = os.path.join(OUTCSV_DIR, csv_filename)
    df.to_csv(csv_file_path, index=False)
    return os.path.join(mount_path, csv_filename)

def generate_and_update_csvs(num_records: int = 80):
    """Generate dummy CSVs and update GeneralQuery table with file paths."""
    try:
        session = default_session()
        count = session.query(GeneralQuery).count()
        if count == 0:
            for i in range(min(num_records, len(all_general_queries))):
                csv_file_path = generate_dummy_csv(query_types[i], all_general_queries[i], i + 1)
                new_general_query = GeneralQuery(
                    query_type=query_types[i],
                    query_sql=all_general_queries[i],
                    description=f"Dummy {query_types[i]} query for analysis {i + 1}",
                    csv_file_path=csv_file_path,
                    created_at=datetime.utcnow()
                )
                session.add(new_general_query)
            session.commit()
            print(f"Successfully generated {min(num_records, len(all_general_queries))} dummy CSV files and loaded GeneralQuery records in {mount_path}.")
        else:
            print("GeneralQuery data already exists. Updating CSVs for existing records.")
            # Update CSVs for existing records
            existing_queries = session.query(GeneralQuery).all()
            for query in existing_queries:
                csv_file_path = generate_dummy_csv(query.query_type, query.query_sql, query.query_id)
                query.csv_file_path = csv_file_path
            session.commit()
            print(f"Updated {len(existing_queries)} CSV files for existing GeneralQuery records in {mount_path}.")
    except Exception as e:
        session.rollback()
        print(f"Error generating CSVs or updating GeneralQuery: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    generate_and_update_csvs(80)