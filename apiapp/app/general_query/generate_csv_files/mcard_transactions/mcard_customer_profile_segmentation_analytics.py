# -*- coding: utf-8 -*-
"""3. Mastercard Customer Profile & Segmentation Analytics (fixed folder & columns)"""
import clickhouse_connect
import pandas as pd
from datetime import datetime
import os

# --- ClickHouse Config ---
clickhouse_host = "57.159.27.80"
clickhouse_port = 8123
clickhouse_database = "fmac_db"
clickhouse_user = "fmacadmin"
clickhouse_password = "fmac*2025"

# --- Initialize Client ---
def initialize_clickhouse_client():
    try:
        client = clickhouse_connect.get_client(
            host=clickhouse_host,
            port=clickhouse_port,
            username=clickhouse_user,
            password=clickhouse_password,
            database=clickhouse_database
        )
        print("✅ ClickHouse client initialized successfully.")
        return client
    except Exception as e:
        print(f"❌ Failed to initialize ClickHouse client: {e}")
        raise

# === Customer Profile Queries (10 KPIs) ===
# Revised queries: Each returns exactly 2 columns (label, value) for Plotly plotting.
# - Limited to top 5-7 for visualization.
# - Values are relative where possible (e.g., percentages, normalized metrics).
# - Fixed aggregation issues (e.g., churn uses subquery for per-customer last_txn).
# - Simplified for 2-column output: x=label (e.g., segment), y=value (e.g., pct).
# - Used available columns for segmentation (customer_segment, age_group, gender, city, etc.).
# === FIXED: Customer Profile Queries with PROPER COLUMN NAMES ===
queries = [

    ("top_segments_txn_share", """
        SELECT
            customer_segment AS "Customer Segment",
            round(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM mcard_transactions), 2) AS "Txn Share (%)"
        FROM mcard_transactions
        GROUP BY customer_segment
        ORDER BY "Txn Share (%)" DESC
        LIMIT 7
    """, "Transaction volume share by top customer segments (%)?",
       "Top 7 Segments - Transaction Share", "bar", "bar,pie"),

    ("top_segments_spend_share", """
        SELECT
            customer_segment AS "Customer Segment",
            round(SUM(amount) * 100.0 / (SELECT SUM(amount) FROM mcard_transactions), 2) AS "Spend Share (%)"
        FROM mcard_transactions
        GROUP BY customer_segment
        ORDER BY "Spend Share (%)" DESC
        LIMIT 7
    """, "Spend share by customer segments (%)?",
       "Top 7 Segments - Spend Share", "bar", "bar,pie"),

    ("age_group_avg_ticket", """
        SELECT
            customer_age_group AS "Age Group",
            round(AVG(amount), 2) AS "Avg Ticket Size"
        FROM mcard_transactions
        GROUP BY customer_age_group
        ORDER BY "Avg Ticket Size" DESC
        LIMIT 6
    """, "Average ticket size by age group",
       "Avg Ticket by Age Group", "bar", "bar,scatter"),

    ("gender_txn_share", """
        SELECT
            customer_gender AS "Gender",
            round(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM mcard_transactions), 2) AS "Txn Share (%)"
        FROM mcard_transactions
        GROUP BY customer_gender
        ORDER BY "Txn Share (%)" DESC
    """, "Transaction share by gender",
       "Gender Distribution", "pie", "pie,bar"),

    ("top_cities_customers", """
        SELECT
            customer_city AS "Customer City",
            round(COUNT(DISTINCT customer_id) * 100.0 / (SELECT COUNT(DISTINCT customer_id) FROM mcard_transactions), 2) AS "Customer Base (%)"
        FROM mcard_transactions
        GROUP BY customer_city
        ORDER BY "Customer Base (%)" DESC
        LIMIT 7
    """, "Customer distribution across top cities (%)",
       "Top 7 Cities - Customer Base", "bar", "bar,pie"),

    ("channel_recent_activity", """
        SELECT
            channel AS "Channel",
            COUNT(*) AS "Recent Transactions (30d)"
        FROM mcard_transactions
        WHERE transaction_date >= today() - INTERVAL 30 DAY
        GROUP BY channel
        ORDER BY "Recent Transactions (30d)" DESC
        LIMIT 6
    """, "Most active channels in last 30 days",
       "Recent Activity by Channel", "bar", "bar"),

    ("segment_churn_rate", """
        SELECT
            customer_segment AS "Customer Segment",
            round(100.0 * COUNT(IF(dateDiff('day', last_txn_date, today()) > 90, 1, NULL)) / COUNT(*), 2) AS "Churn Rate (%)"
        FROM (
            SELECT customer_id, customer_segment, MAX(transaction_date) AS last_txn_date
            FROM mcard_transactions
            GROUP BY customer_id, customer_segment
        )
        GROUP BY customer_segment
        ORDER BY "Churn Rate (%)" DESC
        LIMIT 7
    """, "Churn rate by customer segment (90-day inactivity)",
       "Churn Rate by Segment", "bar", "bar"),

    ("mcc_txn_share", """
        SELECT
            merchant_category_code AS "MCC Category",
            round(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM mcard_transactions), 2) AS "Txn Share (%)"
        FROM mcard_transactions
        GROUP BY merchant_category_code
        ORDER BY "Txn Share (%)" DESC
        LIMIT 7
    """, "Top merchant categories by transaction volume",
       "Top MCC Categories", "pie", "pie,bar"),

    ("peak_hour_by_mcc", """
        SELECT
            merchant_category_code AS "MCC Category",
            argMax(toHour(transaction_date), COUNT(*)) AS "Peak Hour (24h)"
        FROM mcard_transactions
        GROUP BY merchant_category_code
        ORDER BY COUNT(*) DESC
        LIMIT 6
    """, "Peak transaction hour for top MCCs",
       "Peak Hour by MCC", "bar", "bar,line"),

    ("card_type_distribution", """
        SELECT
            card_type AS "Card Type",
            round(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM mcard_transactions), 2) AS "Usage Share (%)"
        FROM mcard_transactions
        GROUP BY card_type
        ORDER BY "Usage Share (%)" DESC
        LIMIT 5
    """, "Card type usage distribution",
       "Card Type Share", "pie", "pie,bar")
]

# === Build Metadata CSV ===
now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def run_customer_profile_queries():
    client = initialize_clickhouse_client()
    output_dir = "general_out_csvfiles"
    output_save_dir = "app/general_out_csvfiles"
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(output_save_dir, exist_ok=True)
    results = []
    for i, (name, sql, q_str, desc, chart, suggested_chart) in enumerate(queries, start=1):
        try:
            result = client.query(sql)
            # Ensure exactly 2 columns for plotting
            if len(result.column_names) != 2:
                raise ValueError(f"Query {i} returned {len(result.column_names)} columns, expected 2")
            plot_df = pd.DataFrame(result.result_rows, columns=result.column_names)
            csv_filename = f"general_query_{i}_{name}.csv"
            csv_save_path = os.path.join(output_save_dir, csv_filename)
            plot_df.to_csv(csv_save_path, index=False, encoding="utf-8")
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