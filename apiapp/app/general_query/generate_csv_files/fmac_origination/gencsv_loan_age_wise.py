# app/general_query/general_queries_run/loan_age.py
import pandas as pd
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
            host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT,
            username=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD,
            database=CLICKHOUSE_DATABASE
        )
        return client
    except Exception as e:
        print(f"Failed to initialize ClickHouse client: {e}")
        raise

queries = [
    ("loans_by_loan_age_bucket", """
        SELECT
            CASE
                WHEN loan_age < 12 THEN '0-1 yr'
                WHEN loan_age BETWEEN 12 AND 36 THEN '1-3 yr'
                WHEN loan_age BETWEEN 37 AND 60 THEN '3-5 yr'
                ELSE '5+ yr'
            END AS loan_age_bucket,
            COUNT(*) AS total_loans
        FROM final_loan_status_proj
        GROUP BY loan_age_bucket
        ORDER BY loan_age_bucket
    """, "Loans by age bucket", "Age distribution", "bar", "bar,pie,histogram"),

    ("avg_upb_by_loan_age_bucket", """
        SELECT
            CASE
                WHEN loan_age < 12 THEN '0-1 yr'
                WHEN loan_age BETWEEN 12 AND 36 THEN '1-3 yr'
                WHEN loan_age BETWEEN 37 AND 60 THEN '3-5 yr'
                ELSE '5+ yr'
            END AS loan_age_bucket,
            ROUND(AVG(current_actual_upb), 2) AS avg_current_upb,
            ROUND(AVG(original_upb), 2) AS avg_original_upb
        FROM final_loan_status_proj
        GROUP BY loan_age_bucket
        ORDER BY loan_age_bucket
    """, "UPB by age", "Balance decay", "bar", "bar,line,scatter"),

    ("avg_loss_severity_by_loan_age", """
        SELECT
            CASE
                WHEN loan_age < 12 THEN '0-1 yr'
                WHEN loan_age BETWEEN 12 AND 36 THEN '1-3 yr'
                WHEN loan_age BETWEEN 37 AND 60 THEN '3-5 yr'
                ELSE '5+ yr'
            END AS loan_age_bucket,
            ROUND(AVG(actual_loss / NULLIF(original_upb, 0)), 3) AS avg_loss_severity
        FROM final_loan_status_proj
        GROUP BY loan_age_bucket
        ORDER BY loan_age_bucket
    """, "Loss severity by age", "Loss curve", "bar", "bar,line,histogram"),

    ("default_rate_by_loan_age", """
        SELECT
            CASE
                WHEN loan_age < 12 THEN '0-1 yr'
                WHEN loan_age BETWEEN 12 AND 36 THEN '1-3 yr'
                WHEN loan_age BETWEEN 37 AND 60 THEN '3-5 yr'
                ELSE '5+ yr'
            END AS loan_age_bucket,
            ROUND(100 * COUNTIf(zero_balance_code IN ('03','09')) / COUNT(*), 2) AS default_rate_percent
        FROM final_loan_status_proj
        GROUP BY loan_age_bucket
        ORDER BY loan_age_bucket
    """, "Default rate by age", "Default curve", "bar", "bar,pie,scatter"),

    ("avg_interest_by_loan_age", """
        SELECT
            CASE
                WHEN loan_age < 12 THEN '0-1 yr'
                WHEN loan_age BETWEEN 12 AND 36 THEN '1-3 yr'
                WHEN loan_age BETWEEN 37 AND 60 THEN '3-5 yr'
                ELSE '5+ yr'
            END AS loan_age_bucket,
            ROUND(AVG(original_interest_rate), 2) AS avg_interest_rate
        FROM final_loan_status_proj
        GROUP BY loan_age_bucket
        ORDER BY loan_age_bucket
    """, "Interest rate by age", "Rate stability", "bar", "bar,line,histogram"),

    ("cost_ratio_by_loan_age", """
        SELECT
            CASE
                WHEN loan_age < 12 THEN '0-1 yr'
                WHEN loan_age BETWEEN 12 AND 36 THEN '1-3 yr'
                WHEN loan_age BETWEEN 37 AND 60 THEN '3-5 yr'
                ELSE '5+ yr'
            END AS loan_age_bucket,
            ROUND(AVG((legal_costs + maintenance_preservation_costs + taxes_insurance)/NULLIF(original_upb, 0)), 4) AS avg_cost_ratio
        FROM final_loan_status_proj
        GROUP BY loan_age_bucket
        ORDER BY loan_age_bucket
    """, "Cost ratio by age", "Cost burden", "bar", "bar,scatter,pie"),

    ("risk_adjusted_yield_by_loan_age", """
        SELECT
            CASE
                WHEN loan_age < 12 THEN '0-1 yr'
                WHEN loan_age BETWEEN 12 AND 36 THEN '1-3 yr'
                WHEN loan_age BETWEEN 37 AND 60 THEN '3-5 yr'
                ELSE '5+ yr'
            END AS loan_age_bucket,
            ROUND(AVG(original_interest_rate) - AVG(actual_loss / NULLIF(original_upb, 0))*100, 2) AS risk_adjusted_yield
        FROM final_loan_status_proj
        GROUP BY loan_age_bucket
        ORDER BY loan_age_bucket
    """, "Risk-adjusted yield by age", "Yield decay", "bar", "bar,line,scatter,histogram")
]

def run_loan_age_queries():
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