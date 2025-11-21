# app/general_query/general_queries_run/delinquency_status.py
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
    ("loans_by_delinquency_status", """
        SELECT current_loan_delinquency_status, COUNT(*) AS total_loans
        FROM final_loan_status_proj
        GROUP BY current_loan_delinquency_status
        ORDER BY total_loans DESC
    """, "How many loans exist by delinquency status?", "Loan count by delinquency status", "bar", "pie,histogram,scatter"),
    
    ("avg_upb_by_delinquency_status", """
        SELECT current_loan_delinquency_status,
               ROUND(AVG(current_actual_upb), 2) AS avg_current_upb,
               ROUND(AVG(original_upb), 2) AS avg_original_upb
        FROM final_loan_status_proj
        GROUP BY current_loan_delinquency_status
        ORDER BY current_loan_delinquency_status
    """, "What is the average current and original UPB by delinquency status?", "Average UPB by delinquency status", "bar", "line,scatter,histogram"),
    
    ("avg_loss_severity_by_delinquency_status", """
        SELECT current_loan_delinquency_status,
               ROUND(AVG(actual_loss / NULLIF(original_upb, 0)), 3) AS avg_loss_severity
        FROM final_loan_status_proj
        GROUP BY current_loan_delinquency_status
        ORDER BY current_loan_delinquency_status
    """, "What is the average loss severity by delinquency status?", "Average loss severity by delinquency status", "line", "bar,pie,scatter"),
    
    ("avg_ltv_dti_by_delinquency_status", """
        SELECT current_loan_delinquency_status,
               ROUND(AVG(original_ltv), 2) AS avg_ltv,
               ROUND(AVG(original_dti_ratio), 2) AS avg_dti
        FROM final_loan_status_proj
        GROUP BY current_loan_delinquency_status
        ORDER BY current_loan_delinquency_status
    """, "What is the average LTV and DTI by delinquency status?", "Average LTV & DTI by delinquency status", "bar", "scatter,line,histogram"),
    
    ("avg_costs_by_delinquency_status", """
        SELECT current_loan_delinquency_status,
               ROUND(AVG(legal_costs), 2) AS avg_legal_costs,
               ROUND(AVG(maintenance_preservation_costs), 2) AS avg_maintenance_costs,
               ROUND(AVG(taxes_insurance), 2) AS avg_taxes_insurance,
               ROUND(AVG(legal_costs + maintenance_preservation_costs + taxes_insurance), 2) AS avg_total_cost
        FROM final_loan_status_proj
        GROUP BY current_loan_delinquency_status
        ORDER BY current_loan_delinquency_status
    """, "What are the average costs (legal, maintenance, taxes) by delinquency status?", "Average costs by delinquency status", "bar", "histogram,line,pie"),
    
    ("risk_adjusted_yield_by_delinquency_status", """
        SELECT current_loan_delinquency_status,
               ROUND(AVG(original_interest_rate) - AVG(actual_loss / NULLIF(original_upb, 0)) * 100, 2) AS risk_adjusted_yield
        FROM final_loan_status_proj
        GROUP BY current_loan_delinquency_status
        ORDER BY current_loan_delinquency_status
    """, "What is the risk-adjusted yield by delinquency status?", "Risk-adjusted yield by delinquency status", "line", "bar,histogram,scatter")
]

def run_delinquency_status_queries():
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