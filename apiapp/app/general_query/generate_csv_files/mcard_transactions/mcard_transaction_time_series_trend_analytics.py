# -*- coding: utf-8 -*-
"""2. Mastercard Transaction Time Series Trend Analytics"""

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


# === Time Series Trend Queries ===
# -*- coding: utf-8 -*-
"""2. Mastercard Transaction Time Series Trend Analytics"""
queries = [
    ("monthly_volume_trend", """
        SELECT 
            formatDateTime(toStartOfMonth(transaction_date), '%b %Y') AS "Month",
            COUNT(*) AS "Transaction Volume"
        FROM mcard_transactions
        GROUP BY toStartOfMonth(transaction_date)
        ORDER BY toStartOfMonth(transaction_date)
    """, "Clean monthly transaction volume trend",
       "Monthly Transaction Volume", "line", "line,bar"),

    ("monthly_spend_trend", """
        SELECT 
            formatDateTime(toStartOfMonth(transaction_date), '%b %Y') AS "Month",
            round(SUM(amount) / 1000000, 2) AS "Total Spend (M)"
        FROM mcard_transactions
        GROUP BY toStartOfMonth(transaction_date)
        ORDER BY toStartOfMonth(transaction_date)
    """, "Clean monthly spend trend in millions",
       "Monthly Spend Trend", "line", "line,bar"),

    ("monthly_txn_volume", """
        SELECT toStartOfMonth(transaction_date) AS "Month",
               COUNT(*) AS "Transaction Count"
        FROM mcard_transactions
        GROUP BY "Month"
        ORDER BY "Month"
    """, "Monthly transaction volume", "Monthly Volume", "bar", "bar,line"),

    ("monthly_spend_trend", """
        SELECT toStartOfMonth(transaction_date) AS "Month",
               round(SUM(amount) / 1000000, 2) AS "Spend (M)"
        FROM mcard_transactions
        GROUP BY "Month"
        ORDER BY "Month"
    """, "Monthly spend trend", "Monthly Spend (Millions)", "bar", "bar,line,area"),

    ("hourly_pattern", """
        SELECT toHour(transaction_date) AS "Hour (24h)",
               COUNT(*) AS "Transactions"
        FROM mcard_transactions
        GROUP BY "Hour (24h)"
        ORDER BY "Hour (24h)"
    """, "Peak transaction hours", "Hourly Activity Pattern", "bar", "bar,line")
]

# === Build Metadata ===
now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def run_transaction_time_series_queries():
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