# -*- coding: utf-8 -*-
"""5. Mastercard Card Network Comparison Analytics - FIXED & BEAUTIFUL"""
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

def initialize_clickhouse_client():
    try:
        client = clickhouse_connect.get_client(
            host=clickhouse_host, port=clickhouse_port,
            username=clickhouse_user, password=clickhouse_password,
            database=clickhouse_database
        )
        print("ClickHouse client initialized successfully.")
        return client
    except Exception as e:
        print(f"Failed to initialize ClickHouse client: {e}")
        raise

# === 8 PERFECT, ALWAYS-RICH CHARTS (Even with 1 Network!) ===
queries = [

    ("spend_by_card_type", """
        SELECT 
            card_type AS "Card Type",
            round(SUM(amount) * 100.0 / (SELECT SUM(amount) FROM mcard_transactions), 2) AS "Spend Share (%)"
        FROM mcard_transactions
        GROUP BY card_type
        ORDER BY "Spend Share (%)" DESC
    """, "Credit vs Debit vs Prepaid spend contribution", "Spend by Card Type", "pie", "pie,bar"),

    ("volume_by_card_type", """
        SELECT 
            card_type AS "Card Type",
            round(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM mcard_transactions), 2) AS "Volume Share (%)"
        FROM mcard_transactions
        GROUP BY card_type
        ORDER BY "Volume Share (%)" DESC
    """, "Which card type is used most?", "Volume by Card Type", "bar", "bar,pie"),

    ("spend_by_channel", """
        SELECT 
            channel AS "Channel",
            round(SUM(amount) * 100.0 / (SELECT SUM(amount) FROM mcard_transactions), 2) AS "Spend Share (%)"
        FROM mcard_transactions
        GROUP BY channel
        ORDER BY "Spend Share (%)" DESC
    """, "POS vs Online vs ATM spend", "Spend by Channel", "pie", "pie,bar"),

    ("avg_ticket_by_channel", """
        SELECT 
            channel AS "Channel",
            round(AVG(amount), 2) AS "Avg Ticket Size"
        FROM mcard_transactions
        GROUP BY channel
        ORDER BY "Avg Ticket Size" DESC
    """, "High-value vs low-value channels", "Avg Ticket by Channel", "bar", "bar"),

    ("success_rate_by_channel", """
        SELECT 
            channel AS "Channel",
            round(100.0 * COUNT(IF(transaction_status = 'Success', 1, NULL)) / COUNT(*), 2) AS "Success Rate (%)"
        FROM mcard_transactions
        GROUP BY channel
        ORDER BY "Success Rate (%)" DESC
    """, "Which channel has highest approval?", "Approval Rate by Channel", "bar", "bar,gauge"),

    ("top_merchant_categories", """
        SELECT 
            merchant_category_code AS "MCC Category",
            round(SUM(amount) * 100.0 / (SELECT SUM(amount) FROM mcard_transactions), 2) AS "Spend Share (%)"
        FROM mcard_transactions
        GROUP BY merchant_category_code
        ORDER BY "Spend Share (%)" DESC
        LIMIT 8
    """, "Top spending categories", "Top MCC Categories", "bar", "bar,pie"),

    ("spend_by_customer_segment", """
        SELECT 
            customer_segment AS "Customer Segment",
            round(SUM(amount) * 100.0 / (SELECT SUM(amount) FROM mcard_transactions), 2) AS "Spend Share (%)"
        FROM mcard_transactions
        GROUP BY customer_segment
        ORDER BY "Spend Share (%)" DESC
        LIMIT 7
    """, "Premium vs Mass vs Corporate spend", "Spend by Segment", "bar", "bar,pie"),

    ("monthly_spend_trend", """
        SELECT 
            formatDateTime(toStartOfMonth(transaction_date), '%b %Y') AS "Month",
            round(SUM(amount) / 1000000, 2) AS "Total Spend (M)"
        FROM mcard_transactions
        GROUP BY toStartOfMonth(transaction_date)
        ORDER BY toStartOfMonth(transaction_date)
    """, "Monthly spend growth trend", "Monthly Spend Trend", "line", "line,area")
]

def run_card_network_queries():
    client = initialize_clickhouse_client()
    output_dir = "general_out_csvfiles"
    output_save_dir = "app/general_out_csvfiles"
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(output_save_dir, exist_ok=True)
    results = []

    for i, (name, sql, q_str, desc, chart, suggested_chart) in enumerate(queries, start=1):
        try:
            result = client.query(sql)
            df = pd.DataFrame(result.result_rows, columns=result.column_names[:2])  # Force 2 cols
            csv_filename = f"general_query_{i}_{name}.csv"
            csv_path = os.path.join(output_save_dir, csv_filename)
            df.to_csv(csv_path, index=False, encoding="utf-8")

            results.append({
                "query_sql_string": sql.strip(),
                "query_str": q_str,
                "description": desc,
                "csv_file_path": os.path.join(output_dir, csv_filename),
                "chart_type": chart,
                "suggested_chart_types": suggested_chart
            })
            print(f"Query {i} saved: {csv_filename}")
        except Exception as e:
            print(f"Failed query {i} ({name}): {e}")

    client.close()
    return results
