# app/general_query/general_queries_run/first_payment_year.py
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
    ("loans_by_origination_year", """
        SELECT first_payment_year, COUNT(*) AS total_loans
        FROM final_loan_status_proj
        GROUP BY first_payment_year
        ORDER BY first_payment_year
    """, "Loan count by origination year", "Vintage volume", "line", "line,bar,histogram"),

    ("avg_upb_by_origination_year", """
        SELECT first_payment_year, ROUND(AVG(original_upb), 2) AS avg_original_upb
        FROM final_loan_status_proj
        GROUP BY first_payment_year
        ORDER BY first_payment_year
    """, "Average UPB by origination year", "Loan size trend", "line", "line,bar,scatter"),

    ("avg_interest_by_origination_year", """
        SELECT first_payment_year, ROUND(AVG(original_interest_rate), 2) AS avg_interest_rate
        FROM final_loan_status_proj
        GROUP BY first_payment_year
        ORDER BY first_payment_year
    """, "Interest rate trend", "Rate over time", "line", "line,scatter,bar"),

    ("avg_ltv_dti_by_origination_year", """
        SELECT first_payment_year,
               ROUND(AVG(original_ltv), 2) AS avg_ltv,
               ROUND(AVG(original_dti_ratio), 2) AS avg_dti
        FROM final_loan_status_proj
        GROUP BY first_payment_year
        ORDER BY first_payment_year
    """, "LTV & DTI trend", "Risk profile", "line", "line,bar,scatter,histogram"),

    ("default_rate_by_origination_year", """
        SELECT first_payment_year,
               ROUND(100 * SUM(CASE WHEN zero_balance_code IN ('03','09') THEN 1 ELSE 0 END)/COUNT(*), 2) AS default_rate_percent
        FROM final_loan_status_proj
        GROUP BY first_payment_year
        ORDER BY first_payment_year
    """, "Default rate by vintage", "Vintage performance", "line", "line,bar,pie"),

    ("delinquency_rate_by_origination_year", """
        SELECT first_payment_year,
               ROUND(100 * COUNTIf(toUInt8OrZero(current_loan_delinquency_status) > 0)/COUNT(*), 2) AS delinquency_rate_percent
        FROM final_loan_status_proj
        GROUP BY first_payment_year
        ORDER BY first_payment_year
    """, "Delinquency by vintage", "Early warning", "line", "line,scatter,histogram"),

    ("avg_loss_severity_by_origination_year", """
        SELECT first_payment_year, ROUND(AVG(actual_loss / NULLIF(original_upb, 0)), 3) AS avg_loss_severity
        FROM final_loan_status_proj
        GROUP BY first_payment_year
        ORDER BY first_payment_year
    """, "Loss severity by vintage", "Loss trend", "line", "line,bar,scatter"),

    ("avg_recovery_rate_by_origination_year", """
        SELECT first_payment_year, ROUND(AVG((original_upb - actual_loss)/NULLIF(original_upb, 0)), 3) AS avg_recovery_rate
        FROM final_loan_status_proj
        GROUP BY first_payment_year
        ORDER BY first_payment_year
    """, "Recovery rate by vintage", "Recovery trend", "line", "line,bar,histogram"),

    ("avg_costs_by_origination_year", """
        SELECT first_payment_year,
               ROUND(AVG(legal_costs + maintenance_preservation_costs), 2) AS avg_total_property_costs
        FROM final_loan_status_proj
        GROUP BY first_payment_year
        ORDER BY first_payment_year
    """, "Property costs by vintage", "Cost trend", "line", "line,scatter,bar"),

    ("risk_adjusted_yield_by_origination_year", """
        SELECT first_payment_year,
               ROUND(AVG(original_interest_rate) - AVG(actual_loss / NULLIF(original_upb, 0))*100, 2) AS risk_adjusted_yield
        FROM final_loan_status_proj
        GROUP BY first_payment_year
        ORDER BY first_payment_year
    """, "Risk-adjusted yield", "Yield vs risk", "line", "line,bar,scatter,histogram")
]

def run_first_payment_year_queries():
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