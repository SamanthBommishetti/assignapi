# app/general_query/general_queries_run/loan_purpose.py
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
    ("loans_by_purpose", """
        SELECT loan_purpose, COUNT(*) AS total_loans
        FROM final_loan_status_proj
        GROUP BY loan_purpose
        ORDER BY total_loans DESC
    """, "How many loans by loan purpose?", "Loan count by purpose", "bar", "bar,pie,histogram"),

    ("avg_original_upb_by_purpose", """
        SELECT loan_purpose, ROUND(AVG(original_upb), 2) AS avg_original_upb
        FROM final_loan_status_proj
        GROUP BY loan_purpose
        ORDER BY avg_original_upb DESC
    """, "Average original UPB by purpose", "Avg loan size", "bar", "bar,scatter,line"),

    ("avg_interest_rate_by_purpose", """
        SELECT loan_purpose, ROUND(AVG(original_interest_rate), 2) AS avg_interest_rate
        FROM final_loan_status_proj
        GROUP BY loan_purpose
        ORDER BY avg_interest_rate DESC
    """, "Average interest rate by purpose", "Avg rate", "bar", "bar,histogram,pie"),

    ("default_rate_by_purpose", """
        SELECT loan_purpose,
               ROUND(100 * SUM(CASE WHEN zero_balance_code IN ('03','09') THEN 1 ELSE 0 END)/COUNT(*), 2) AS default_rate_percent
        FROM final_loan_status_proj
        GROUP BY loan_purpose
        ORDER BY default_rate_percent DESC
    """, "Default rate by purpose", "Default %", "bar", "bar,line,scatter"),

    ("delinquency_rate_by_purpose", """
        SELECT loan_purpose,
               ROUND(100 * COUNTIf(current_loan_delinquency_status != '0') / COUNT(*), 2) AS delinquency_rate_percent
        FROM final_loan_status_proj
        GROUP BY loan_purpose
        ORDER BY delinquency_rate_percent DESC
    """, "Delinquency rate by purpose", "Delinquency %", "bar", "bar,pie,histogram"),

    ("avg_ltv_dti_by_purpose", """
        SELECT loan_purpose,
               ROUND(AVG(original_ltv), 2) AS avg_ltv,
               ROUND(AVG(original_dti_ratio), 2) AS avg_dti
        FROM final_loan_status_proj
        GROUP BY loan_purpose
        ORDER BY avg_ltv DESC
    """, "Avg LTV & DTI by purpose", "Risk metrics", "bar", "bar,scatter,line"),

    ("avg_loan_age_by_purpose", """
        SELECT loan_purpose, ROUND(AVG(loan_age), 2) AS avg_loan_age
        FROM final_loan_status_proj
        GROUP BY loan_purpose
        ORDER BY avg_loan_age DESC
    """, "Average loan age by purpose", "Loan age", "bar", "bar,histogram,pie"),

    ("avg_loss_severity_by_purpose", """
        SELECT loan_purpose, ROUND(AVG(actual_loss / NULLIF(original_upb, 0)), 3) AS avg_loss_severity
        FROM final_loan_status_proj
        GROUP BY loan_purpose
        ORDER BY avg_loss_severity DESC
    """, "Loss severity by purpose", "Loss severity", "bar", "bar,scatter,line"),

    ("avg_costs_by_purpose", """
        SELECT loan_purpose,
               ROUND(AVG(legal_costs + maintenance_preservation_costs), 2) AS avg_total_property_costs
        FROM final_loan_status_proj
        GROUP BY loan_purpose
        ORDER BY avg_total_property_costs DESC
    """, "Avg property costs by purpose", "Cost analysis", "bar", "bar,histogram,scatter"),

    ("high_risk_concentration_by_purpose", """
        SELECT loan_purpose,
               COUNTIf(original_dti_ratio > 45 OR original_ltv > 90) AS high_risk_loans,
               ROUND(100 * COUNTIf(original_dti_ratio > 45 OR original_ltv > 90) / COUNT(*), 2) AS high_risk_ratio
        FROM final_loan_status_proj
        GROUP BY loan_purpose
        ORDER BY high_risk_ratio DESC
    """, "High-risk loans (LTV>90 or DTI>45)", "Risk concentration", "bar", "bar,pie,scatter")
]

def run_loan_purpose_queries():
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