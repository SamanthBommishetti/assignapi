# app/general_query/general_queries_run/combined_kpi.py
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
    ("portfolio_summary_by_property_type_loan_purpose", """
        SELECT
            property_type,
            loan_purpose,
            COUNT(*) AS total_loans,
            ROUND(AVG(original_upb),2) AS avg_loan_amount,
            ROUND(AVG(original_interest_rate),2) AS avg_interest_rate,
            ROUND(AVG(original_ltv),2) AS avg_ltv,
            ROUND(AVG(original_dti_ratio),2) AS avg_dti,
            ROUND(AVG(actual_loss/NULLIF(original_upb,0)),3) AS avg_loss_severity
        FROM final_loan_status_proj
        GROUP BY property_type, loan_purpose
        ORDER BY property_type, total_loans DESC
    """, "What is the portfolio summary by property type & loan purpose?", "Portfolio summary by property type & loan purpose", "bar", "bar,pie,scatter,histogram"),
    
    ("portfolio_summary_by_origination_year_property_type", """
        SELECT
            first_payment_year AS origination_year,
            property_type,
            COUNT(*) AS total_loans,
            ROUND(AVG(original_upb),2) AS avg_loan_amount,
            ROUND(AVG(original_interest_rate),2) AS avg_interest_rate,
            ROUND(AVG(actual_loss/NULLIF(original_upb,0)),3) AS avg_loss_severity,
            ROUND(AVG(legal_costs + maintenance_preservation_costs + taxes_insurance),2) AS avg_total_cost
        FROM final_loan_status_proj
        GROUP BY origination_year, property_type
        ORDER BY origination_year, property_type
    """, "What is the portfolio summary by origination year & property type?", "Portfolio summary by origination year & property type", "bar", "line,bar,scatter"),
    
    ("kpi_summary_by_state_year", """
        SELECT
            property_state,
            first_payment_year AS origination_year,
            ROUND(AVG(original_interest_rate),2) AS avg_interest_rate,
            ROUND(100 * SUM(CASE WHEN zero_balance_code IN ('03','09') THEN 1 ELSE 0 END) / COUNT(*),2) AS default_rate_percent,
            ROUND(AVG(actual_loss/NULLIF(original_upb,0)),3) AS avg_loss_severity,
            ROUND(AVG((legal_costs + maintenance_preservation_costs + taxes_insurance)/NULLIF(original_upb,0)),4) AS avg_cost_ratio
        FROM final_loan_status_proj
        GROUP BY property_state, origination_year
        ORDER BY property_state, origination_year
    """, "What is the KPI summary (avg rate, default %, loss severity, cost ratio) by state & year?", "KPI summary by state & year", "bar", "scatter,bar,histogram,pie"),
    
    ("vintage_loss_curve", """
        SELECT
            first_payment_year AS origination_year,
            loan_age,
            ROUND(AVG(actual_loss/NULLIF(original_upb,0)),3) AS avg_loss_severity,
            COUNT(*) AS loan_count
        FROM final_loan_status_proj
        GROUP BY origination_year, loan_age
        ORDER BY origination_year, loan_age
    """, "What is the vintage loss curve: avg loss severity vs loan age by origination year?", "Vintage loss curve", "line", "line,scatter,bar,histogram"),
    
    ("yield_risk_map_by_property_type", """
        SELECT
            property_type,
            ROUND(AVG(original_interest_rate),2) AS avg_interest_rate,
            ROUND(AVG(actual_loss/NULLIF(original_upb,0)),3) AS avg_loss_severity,
            COUNT(*) AS total_loans
        FROM final_loan_status_proj
        GROUP BY property_type
        ORDER BY avg_interest_rate DESC
    """, "What is the yield-risk map: interest rate vs loss severity by property type?", "Yield-risk map by property type", "scatter", "scatter,bar,pie,line")
]

def run_combined_kpi_queries():
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