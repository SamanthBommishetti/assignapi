# app/general_query/general_queries_run/maturity_year.py
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
    ("loans_by_maturity_year", """
        SELECT maturity_year, COUNT(*) AS total_loans
        FROM final_loan_status_proj
        GROUP BY maturity_year
        ORDER BY maturity_year
    """, "Loan count by maturity year", "Maturity distribution", "bar", "bar,line,histogram"),

    ("total_balance_by_maturity_year", """
        SELECT maturity_year, SUM(current_actual_upb) AS total_balance
        FROM final_loan_status_proj
        GROUP BY maturity_year
        ORDER BY maturity_year
    """, "Total balance by maturity", "Balance curve", "line", "line,bar,scatter"),

    ("avg_interest_by_maturity_year", """
        SELECT maturity_year, ROUND(AVG(original_interest_rate), 2) AS avg_interest_rate
        FROM final_loan_status_proj
        GROUP BY maturity_year
        ORDER BY maturity_year
    """, "Interest rate by maturity", "Rate trend", "line", "line,scatter,histogram"),

    ("default_rate_by_maturity_year", """
        SELECT maturity_year,
               ROUND(100 * SUM(CASE WHEN zero_balance_code IN ('03','09') THEN 1 ELSE 0 END)/COUNT(*), 2) AS default_rate_percent
        FROM final_loan_status_proj
        GROUP BY maturity_year
        ORDER BY maturity_year
    """, "Default rate by maturity", "Default risk", "line", "line,bar,pie"),

    ("loss_severity_by_maturity_year", """
        SELECT maturity_year, ROUND(AVG(actual_loss / NULLIF(original_upb, 0)), 3) AS avg_loss_severity
        FROM final_loan_status_proj
        GROUP BY maturity_year
        ORDER BY maturity_year
    """, "Loss severity by maturity", "Loss trend", "line", "line,bar,scatter"),

    ("avg_costs_by_maturity_year", """
        SELECT maturity_year,
               ROUND(AVG(legal_costs + maintenance_preservation_costs), 2) AS avg_total_property_costs
        FROM final_loan_status_proj
        GROUP BY maturity_year
        ORDER BY maturity_year
    """, "Property costs by maturity", "Cost curve", "line", "line,histogram,bar"),

    ("avg_loan_age_by_remaining_term_bucket", """
        SELECT
            CASE
                WHEN remaining_months_to_legal_maturity <= 60 THEN '≤5 yrs'
                WHEN remaining_months_to_legal_maturity <= 120 THEN '6-10 yrs'
                WHEN remaining_months_to_legal_maturity <= 180 THEN '11-15 yrs'
                ELSE '>15 yrs'
            END AS remaining_term_bucket,
            ROUND(AVG(loan_age), 2) AS avg_loan_age,
            COUNT(*) AS loan_count
        FROM final_loan_status_proj
        GROUP BY remaining_term_bucket
        ORDER BY remaining_term_bucket
    """, "Avg age by remaining term", "Term analysis", "bar", "bar,scatter,pie"),

    ("avg_ltv_dti_by_remaining_term_bucket", """
        SELECT
            CASE
                WHEN remaining_months_to_legal_maturity <= 60 THEN '≤5 yrs'
                WHEN remaining_months_to_legal_maturity <= 120 THEN '6-10 yrs'
                WHEN remaining_months_to_legal_maturity <= 180 THEN '11-15 yrs'
                ELSE '>15 yrs'
            END AS remaining_term_bucket,
            ROUND(AVG(original_ltv), 2) AS avg_ltv,
            ROUND(AVG(original_dti_ratio), 2) AS avg_dti
        FROM final_loan_status_proj
        GROUP BY remaining_term_bucket
        ORDER BY remaining_term_bucket
    """, "LTV/DTI by remaining term", "Risk by term", "bar", "bar,line,scatter,histogram")
]

def run_maturity_year_queries():
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