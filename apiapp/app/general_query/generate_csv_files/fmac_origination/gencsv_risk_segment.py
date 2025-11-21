# app/general_query/general_queries_run/risk_segment.py
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
    ("loans_by_ltv_band", """
        SELECT
            CASE
                WHEN original_ltv <= 60 THEN '≤60'
                WHEN original_ltv <= 80 THEN '61-80'
                WHEN original_ltv <= 90 THEN '81-90'
                ELSE '>90'
            END AS ltv_band,
            COUNT(*) AS total_loans
        FROM final_loan_status_proj
        GROUP BY ltv_band
        ORDER BY ltv_band
    """, "What is the loan count by LTV band?", "Loan count by LTV band", "bar", "bar,pie,histogram"),
    
    ("avg_interest_by_ltv_band", """
        SELECT
            CASE
                WHEN original_ltv <= 60 THEN '≤60'
                WHEN original_ltv <= 80 THEN '61-80'
                WHEN original_ltv <= 90 THEN '81-90'
                ELSE '>90'
            END AS ltv_band,
            ROUND(AVG(original_interest_rate),2) AS avg_interest_rate
        FROM final_loan_status_proj
        GROUP BY ltv_band
        ORDER BY ltv_band
    """, "What is the average interest rate by LTV band?", "Average interest rate by LTV band", "bar", "bar,line,scatter"),
    
    ("default_rate_by_ltv_band", """
        SELECT
            CASE
                WHEN original_ltv <= 60 THEN '≤60'
                WHEN original_ltv <= 80 THEN '61-80'
                WHEN original_ltv <= 90 THEN '81-90'
                ELSE '>90'
            END AS ltv_band,
            ROUND(100 * SUM(CASE WHEN zero_balance_code IN ('03','09') THEN 1 ELSE 0 END)/COUNT(*),2) AS default_rate_percent
        FROM final_loan_status_proj
        GROUP BY ltv_band
        ORDER BY ltv_band
    """, "What is the default rate by LTV band?", "Default rate by LTV band", "bar", "bar,pie,histogram"),
    
    ("loss_severity_by_ltv_band", """
        SELECT
            CASE
                WHEN original_ltv <= 60 THEN '≤60'
                WHEN original_ltv <= 80 THEN '61-80'
                WHEN original_ltv <= 90 THEN '81-90'
                ELSE '>90'
            END AS ltv_band,
            ROUND(AVG(actual_loss/NULLIF(original_upb,0)),3) AS avg_loss_severity
        FROM final_loan_status_proj
        GROUP BY ltv_band
        ORDER BY ltv_band
    """, "What is the loss severity by LTV band?", "Loss severity by LTV band", "bar", "bar,scatter,line"),
    
    ("loans_by_dti_band", """
        SELECT
            CASE
                WHEN original_dti_ratio <= 35 THEN '≤35'
                WHEN original_dti_ratio <= 45 THEN '36-45'
                ELSE '>45'
            END AS dti_band,
            COUNT(*) AS total_loans
        FROM final_loan_status_proj
        GROUP BY dti_band
        ORDER BY dti_band
    """, "What is the loan count by DTI band?", "Loan count by DTI band", "bar", "bar,pie,scatter"),
    
    ("default_rate_by_dti_band", """
        SELECT
            CASE
                WHEN original_dti_ratio <= 35 THEN '≤35'
                WHEN original_dti_ratio <= 45 THEN '36-45'
                ELSE '>45'
            END AS dti_band,
            ROUND(100 * SUM(CASE WHEN zero_balance_code IN ('03','09') THEN 1 ELSE 0 END)/COUNT(*),2) AS default_rate_percent
        FROM final_loan_status_proj
        GROUP BY dti_band
        ORDER BY dti_band
    """, "What is the default rate by DTI band?", "Default rate by DTI band", "bar", "bar,histogram,line"),
    
    ("high_risk_ltv_dti_intersection", """
        SELECT COUNT(*) AS high_risk_loans
        FROM final_loan_status_proj
        WHERE original_ltv > 90 AND original_dti_ratio > 45
    """, "What is the count of loans that are high-risk in both LTV >90 and DTI >45?", "High-risk loans count (LTV & DTI)", "single_value", "single_value,bar,pie"),
    
    ("avg_cost_ratio_by_ltv_dti_band", """
        SELECT
            CASE
                WHEN original_ltv <= 60 THEN '≤60'
                WHEN original_ltv <= 80 THEN '61-80'
                WHEN original_ltv <= 90 THEN '81-90'
                ELSE '>90'
            END AS ltv_band,
            CASE
                WHEN original_dti_ratio <= 35 THEN '≤35'
                WHEN original_dti_ratio <= 45 THEN '36-45'
                ELSE '>45'
            END AS dti_band,
            ROUND(AVG((legal_costs + maintenance_preservation_costs + taxes_insurance)/NULLIF(original_upb,0)),4) AS avg_cost_ratio,
            COUNT(*) AS loan_count
        FROM final_loan_status_proj
        GROUP BY ltv_band, dti_band
        ORDER BY ltv_band, dti_band
    """, "What is the average cost ratio (legal+maintenance+tax) by LTV/DTI bands?", "Average cost ratio by LTV/DTI bands", "scatter", "scatter,bar,histogram,line")
]

def run_risk_segment_queries():
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