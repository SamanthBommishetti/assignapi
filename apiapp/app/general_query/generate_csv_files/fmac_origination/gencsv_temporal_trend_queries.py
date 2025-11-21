# app/general_query/general_queries_run/temporal_trend.py
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
    ("total_loans_by_month", """
        SELECT monthly_reporting_period, COUNT(*) AS total_loans
        FROM final_loan_status_proj
        GROUP BY monthly_reporting_period
        ORDER BY monthly_reporting_period
    """, "What is the total loan count by reporting month?", "Total loans by month", "line", "line,bar,scatter"),
    
    ("total_upb_by_month", """
        SELECT monthly_reporting_period, SUM(current_actual_upb) AS total_current_upb
        FROM final_loan_status_proj
        GROUP BY monthly_reporting_period
        ORDER BY monthly_reporting_period
    """, "What is the total current UPB by reporting month?", "Total UPB by month", "line", "line,histogram,bar"),
    
    ("default_rate_trend_over_time", """
        SELECT monthly_reporting_period,
               ROUND(100 * COUNTIf(zero_balance_code IN ('03','09')) / COUNT(*), 2) AS default_rate_percent
        FROM final_loan_status_proj
        GROUP BY monthly_reporting_period
        ORDER BY monthly_reporting_period
    """, "What is the default rate trend over time?", "Default rate trend", "line", "line,scatter,pie"),
    
    ("loss_severity_trend_over_time", """
        SELECT monthly_reporting_period,
               ROUND(AVG(actual_loss / NULLIF(original_upb, 0)), 3) AS avg_loss_severity
        FROM final_loan_status_proj
        GROUP BY monthly_reporting_period
        ORDER BY monthly_reporting_period
    """, "What is the loss severity trend over time?", "Loss severity trend", "line", "line,bar,histogram"),
    
    ("avg_ltv_dti_trend_over_time", """
        SELECT monthly_reporting_period,
               ROUND(AVG(original_ltv), 2) AS avg_ltv,
               ROUND(AVG(original_dti_ratio), 2) AS avg_dti
        FROM final_loan_status_proj
        GROUP BY monthly_reporting_period
        ORDER BY monthly_reporting_period
    """, "What is the average LTV & DTI trend over time?", "Avg LTV & DTI trend", "line", "line,scatter,bar"),
    
    ("expense_trend_over_time", """
        SELECT monthly_reporting_period,
               ROUND(SUM(legal_costs), 2) AS total_legal_costs,
               ROUND(SUM(maintenance_preservation_costs), 2) AS total_maintenance_costs,
               ROUND(SUM(taxes_insurance), 2) AS total_taxes_insurance,
               ROUND(SUM(legal_costs + maintenance_preservation_costs + taxes_insurance), 2) AS total_expense
        FROM final_loan_status_proj
        GROUP BY monthly_reporting_period
        ORDER BY monthly_reporting_period
    """, "What is the expense trend over time (legal + maintenance + taxes)?", "Expense trend", "line", "line,bar,scatter,histogram"),
    
    ("risk_adjusted_yield_trend", """
        SELECT monthly_reporting_period,
               ROUND(AVG(original_interest_rate) - AVG(actual_loss / NULLIF(original_upb, 0)) * 100, 2) AS risk_adjusted_yield
        FROM final_loan_status_proj
        GROUP BY monthly_reporting_period
        ORDER BY monthly_reporting_period
    """, "What is the risk-adjusted yield trend over time?", "Risk-adjusted yield trend", "line", "line,pie,bar")
]

def run_temporal_trend_queries():
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