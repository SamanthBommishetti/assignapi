# app/general_query/general_queries_run/property_type.py
import pandas as pd
from datetime import datetime
import clickhouse_connect
import os


CLICKHOUSE_HOST = "57.159.27.80"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_DATABASE = "fmac_db"
CLICKHOUSE_USER = "fmacadmin"
CLICKHOUSE_PASSWORD = "fmac*2025"

def initialize_clickhouse_client():
    try:
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            database=CLICKHOUSE_DATABASE
        )
        return client
    except Exception as e:
        print(f"Failed to initialize ClickHouse client: {e}")
        raise

queries = [
    ("loans_by_state", """
        SELECT property_state, COUNT(*) AS total_loans
        FROM final_loan_status_proj
        GROUP BY property_state
        ORDER BY total_loans DESC
    """, "How many loans exist by property state?", "Loans count by state", "bar", "bar,histogram,pie"),
    
    ("loans_by_property_type", """
        SELECT property_type, COUNT(*) AS total_loans
        FROM final_loan_status_proj
        GROUP BY property_type
        ORDER BY total_loans DESC
    """, "How many loans exist by property type?", "Loans count by property type", "bar", "bar,pie,scatter"),
    
    ("avg_original_upb_by_state", """
        SELECT property_state, AVG(original_upb) AS avg_original_upb
        FROM final_loan_status_proj
        GROUP BY property_state
        ORDER BY avg_original_upb DESC
    """, "What is the average original UPB by state?", "Average loan balance by state", "bar", "bar,line,histogram"),
    
    ("avg_interest_rate_by_property_type", """
        SELECT property_type, ROUND(AVG(original_interest_rate), 2) AS avg_interest_rate
        FROM final_loan_status_proj
        GROUP BY property_type
        ORDER BY avg_interest_rate DESC
    """, "What is the average interest rate by property type?", "Avg rate by property type", "bar", "bar,scatter,pie"),
    
    ("total_current_balance_by_state", """
        SELECT property_state, SUM(current_actual_upb) AS total_current_balance
        FROM final_loan_status_proj
        GROUP BY property_state
        ORDER BY total_current_balance DESC
    """, "What is the total current loan balance by state?", "Current balance by state", "bar", "bar,histogram,line"),
    
    ("avg_actual_loss_by_state", """
        SELECT property_state, ROUND(AVG(actual_loss), 2) AS avg_actual_loss
        FROM final_loan_status_proj
        GROUP BY property_state
        ORDER BY avg_actual_loss DESC
    """, "What is the average actual loss by state?", "Avg loss by state", "bar", "bar,scatter,pie"),
    
    ("property_distribution_by_state", """
        SELECT property_state, property_type, COUNT(*) AS loan_count
        FROM final_loan_status_proj
        GROUP BY property_state, property_type
        ORDER BY property_state, loan_count DESC
    """, "What is the property type distribution within each state?", "Property type count per state", "bar", "bar,pie,histogram"),
    
    ("avg_loan_age_by_state", """
        SELECT property_state, ROUND(AVG(loan_age), 2) AS avg_loan_age
        FROM final_loan_status_proj
        GROUP BY property_state
        ORDER BY avg_loan_age DESC
    """, "What is the average loan age by state?", "Avg loan age", "bar", "bar,line,scatter"),
    
    ("avg_ltv_by_property_type", """
        SELECT property_type, ROUND(AVG(original_ltv), 2) AS avg_ltv
        FROM final_loan_status_proj
        GROUP BY property_type
        ORDER BY avg_ltv DESC
    """, "What is the average LTV by property type?", "Avg LTV by property type", "bar", "bar,histogram,pie"),
    
    ("avg_dti_by_state", """
        SELECT property_state, ROUND(AVG(original_dti_ratio), 2) AS avg_dti
        FROM final_loan_status_proj
        GROUP BY property_state
        ORDER BY avg_dti DESC
    """, "What is the average DTI by state?", "Avg DTI ratio by state", "bar", "bar,scatter,line"),
    
    ("avg_legal_costs_by_state", """
        SELECT property_state, ROUND(AVG(legal_costs), 2) AS avg_legal_costs
        FROM final_loan_status_proj
        GROUP BY property_state
        ORDER BY avg_legal_costs DESC
    """, "What is the average legal cost by state?", "Avg legal cost", "bar", "bar,histogram,pie"),
    
    ("total_maintenance_cost_by_state", """
        SELECT property_state, SUM(maintenance_preservation_costs) AS total_maintenance_cost
        FROM final_loan_status_proj
        GROUP BY property_state
        ORDER BY total_maintenance_cost DESC
    """, "What is the total maintenance cost by state?", "Total maintenance cost", "bar", "bar,line,scatter"),
    
    ("total_taxes_insurance_by_property_type", """
        SELECT property_type, SUM(taxes_insurance) AS total_taxes_insurance
        FROM final_loan_status_proj
        GROUP BY property_type
        ORDER BY total_taxes_insurance DESC
    """, "What is the total taxes and insurance cost by property type?", "Taxes & insurance total", "bar", "bar,pie,histogram"),
    
    ("combined_summary_state_property_type", """
        SELECT
            property_state,
            property_type,
            COUNT(*) AS total_loans,
            ROUND(AVG(original_upb), 2) AS avg_loan_amount,
            ROUND(AVG(original_interest_rate), 2) AS avg_rate,
            ROUND(AVG(original_ltv), 2) AS avg_ltv
        FROM final_loan_status_proj
        GROUP BY property_state, property_type
        ORDER BY property_state, total_loans DESC
    """, "What are summary metrics by state and property type?", "Combined summary", "bar", "bar,scatter,line"),
    
    ("default_rate_by_property_type", """
        SELECT
            property_type,
            ROUND(100 * SUM(CASE WHEN zero_balance_code IN ('03', '09') THEN 1 ELSE 0 END) / COUNT(*), 2) AS default_rate_percent
        FROM final_loan_status_proj
        GROUP BY property_type
        ORDER BY default_rate_percent DESC
    """, "What is the default rate by property type?", "Default rate % by property type", "bar", "bar,pie,scatter"),
    
    ("avg_loss_severity_by_state", """
        SELECT
            property_state,
            ROUND(AVG(actual_loss / NULLIF(original_upb, 0)), 3) AS avg_loss_severity
        FROM final_loan_status_proj
        GROUP BY property_state
        ORDER BY avg_loss_severity DESC
    """, "What is the average loss severity by state?", "Avg loss severity", "bar", "bar,histogram,line"),
    
    ("high_ltv_risk_by_state", """
        SELECT
            property_state,
            COUNTIf(original_ltv > 90) AS high_ltv_loans,
            ROUND(100 * COUNTIf(original_ltv > 90) / COUNT(*), 2) AS high_ltv_ratio
        FROM final_loan_status_proj
        GROUP BY property_state
        ORDER BY high_ltv_ratio DESC
    """, "What percentage of loans have high LTV (>90%) by state?", "High LTV concentration", "bar", "bar,pie,scatter"),
    
    ("loan_concentration_by_type_within_state", """
        SELECT
            property_state,
            property_type,
            COUNT(*) AS total_loans,
            ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY property_state), 2) AS pct_within_state
        FROM final_loan_status_proj
        GROUP BY property_state, property_type
        ORDER BY property_state, pct_within_state DESC
    """, "What is the loan concentration by property type within each state?", "Loan concentration within state", "bar", "bar,histogram,pie"),
    
    ("avg_legal_maintenance_cost_by_state", """
        SELECT
            property_state,
            ROUND(AVG(legal_costs + maintenance_preservation_costs), 2) AS avg_property_management_cost
        FROM final_loan_status_proj
        GROUP BY property_state
        ORDER BY avg_property_management_cost DESC
    """, "What is the average combined legal and maintenance cost by state?", "Avg property management cost", "bar", "bar,scatter,line")
]

def run_property_type_queries():
    client = initialize_clickhouse_client()
    output_dir = "general_out_csvfiles"
    output_save_dir = "app/general_out_csvfiles"
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(output_save_dir, exist_ok=True)
    results = []
    for i, (name, sql, q_str, desc, chart, suggested_chart) in enumerate(queries, start=1):
        try:
            result = client.query(sql)
            result_df = pd.DataFrame(result.result_rows, columns=result.column_names)
            csv_filename = f"general_query_{i}_{name}.csv"
            csv_save_path = os.path.join(output_save_dir, csv_filename)
            result_df.to_csv(csv_save_path, index=False, encoding="utf-8")
            csv_table_path = os.path.join(output_dir, csv_filename)
            results.append({
                "query_sql_string": sql.strip(),
                "query_str": q_str,
                "description": desc,
                "csv_file_path": csv_table_path,
                "chart_type": chart,
                "suggested_chart_types": suggested_chart
            })
            print(f"Query {i} saved: {csv_save_path}")
        except Exception as e:
            print(f"Failed query {i}: {e}")
    client.close()
    return results