# app/general_query/general_queries_run/step_modification.py
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
    ("loan_count_by_modification_flag", """
        SELECT step_modification_flag, COUNT(*) AS total_loans
        FROM final_loan_status_proj
        GROUP BY step_modification_flag
        ORDER BY total_loans DESC
    """, "What is the count of modified vs non-modified loans?", "Count of modified vs non-modified loans", "bar", "bar,pie,histogram"),
    
    ("default_rate_by_modification_flag", """
        SELECT
            step_modification_flag,
            ROUND(100 * COUNTIf(zero_balance_code IN ('03','09')) / COUNT(*), 2) AS default_rate_percent
        FROM final_loan_status_proj
        GROUP BY step_modification_flag
        ORDER BY step_modification_flag
    """, "What is the default rate among modified vs unmodified loans?", "Default rate by modification flag", "bar", "bar,scatter,line"),
    
    ("avg_interest_rate_by_modification_flag", """
        SELECT
            step_modification_flag,
            ROUND(AVG(original_interest_rate), 2) AS avg_interest_rate
        FROM final_loan_status_proj
        GROUP BY step_modification_flag
        ORDER BY step_modification_flag
    """, "What is the average interest rate difference (modified vs not)?", "Average interest rate by modification flag", "bar", "bar,histogram,pie"),
    
    ("avg_loss_severity_by_modification_flag", """
        SELECT
            step_modification_flag,
            ROUND(AVG(actual_loss / NULLIF(original_upb, 0)), 3) AS avg_loss_severity
        FROM final_loan_status_proj
        GROUP BY step_modification_flag
        ORDER BY step_modification_flag
    """, "What is the average loss severity (modified vs not)?", "Average loss severity by modification flag", "bar", "bar,line,scatter"),
    
    ("avg_costs_by_modification_flag", """
        SELECT
            step_modification_flag,
            ROUND(AVG(legal_costs), 2) AS avg_legal_costs,
            ROUND(AVG(maintenance_preservation_costs), 2) AS avg_maintenance_costs,
            ROUND(AVG(taxes_insurance), 2) AS avg_taxes_insurance,
            ROUND(AVG(legal_costs + maintenance_preservation_costs + taxes_insurance), 2) AS avg_total_cost
        FROM final_loan_status_proj
        GROUP BY step_modification_flag
        ORDER BY step_modification_flag
    """, "What are the average costs (legal, maintenance, taxes) by modification status?", "Average costs by modification flag", "bar", "bar,histogram,line"),
    
    ("loan_count_by_status", """
        SELECT
            loan_status,
            COUNT(*) AS total_loans
        FROM final_loan_status_proj
        GROUP BY loan_status
        ORDER BY total_loans DESC
    """, "What is the loan count by overall loan status (active, paid off, defaulted)?", "Loan count by status", "bar", "bar,pie,scatter")
]

def run_step_modification_queries():
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