# -*- coding: utf-8 -*-
"""4. Mastercard Merchant Spend & Performance Analytics"""
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


# === Merchant Spend & Performance Queries (10 KPIs) ===
# -*- coding: utf-8 -*-
"""4. Mastercard Merchant Spend & Performance Analytics"""
queries = [
    ("top_merchants_spend", """
        SELECT merchant_name AS "Merchant",
               round(SUM(amount), 0) AS "Total Spend"
        FROM mcard_transactions
        GROUP BY merchant_name
        ORDER BY "Total Spend" DESC
        LIMIT 10
    """, "Top 10 merchants by total spend", "Merchant Spend Leaderboard", "bar", "bar,table"),

    ("top_merchants_volume", """
        SELECT merchant_name AS "Merchant",
               COUNT(*) AS "Transaction Count"
        FROM mcard_transactions
        GROUP BY merchant_name
        ORDER BY "Transaction Count" DESC
        LIMIT 10
    """, "Most transacted merchants", "Top Merchants by Volume", "bar", "bar"),

    ("mcc_spend_share", """
        SELECT merchant_category_code AS "MCC Category",
               round(SUM(amount) * 100.0 / (SELECT SUM(amount) FROM mcard_transactions), 2) AS "Spend Share (%)"
        FROM mcard_transactions
        GROUP BY merchant_category_code
        ORDER BY "Spend Share (%)" DESC
        LIMIT 8
    """, "Which categories dominate spend?", "MCC Spend Contribution", "pie", "pie,bar"),

    ("mcc_txn_share", """
        SELECT merchant_category_code AS "MCC Category",
               round(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM mcard_transactions), 2) AS "Txn Share (%)"
        FROM mcard_transactions
        GROUP BY merchant_category_code
        ORDER BY "Txn Share (%)" DESC
        LIMIT 8
    """, "Most popular merchant categories", "MCC Volume Share", "bar", "bar,pie"),

    ("avg_ticket_by_mcc", """
        SELECT merchant_category_code AS "MCC Category",
               round(AVG(amount), 2) AS "Avg Ticket Size"
        FROM mcard_transactions
        GROUP BY merchant_category_code
        ORDER BY "Avg Ticket Size" DESC
        LIMIT 8
    """, "High vs low value categories", "Avg Ticket by MCC", "bar", "bar,scatter"),

    ("city_spend_share", """
        SELECT customer_city AS "City",
               round(SUM(amount) * 100.0 / (SELECT SUM(amount) FROM mcard_transactions), 2) AS "Spend Share (%)"
        FROM mcard_transactions
        GROUP BY customer_city
        ORDER BY "Spend Share (%)" DESC
        LIMIT 7
    """, "Geographic spend concentration", "Top Cities by Spend", "bar", "bar,pie"),

    ("channel_preference", """
        SELECT channel AS "Channel",
               round(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM mcard_transactions), 2) AS "Usage Share (%)"
        FROM mcard_transactions
        GROUP BY channel
        ORDER BY "Usage Share (%)" DESC
    """, "POS vs Online vs ATM usage", "Channel Preference", "pie", "pie,bar")
]

# === Build Metadata ===
now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def run_merchant_spend_queries():
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