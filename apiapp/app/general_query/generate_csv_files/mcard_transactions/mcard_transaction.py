# -*- coding: utf-8 -*-
"""1. Mastercard Transaction Portfolio Overview Analytics (Plotly-Ready)"""
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
            host=clickhouse_host,
            port=clickhouse_port,
            username=clickhouse_user,
            password=clickhouse_password,
            database=clickhouse_database
        )
        print("ClickHouse client initialized successfully.")
        return client
    except Exception as e:
        print(f"Failed to initialize ClickHouse client: {e}")
        raise

# === 8 Perfect Plotly-Ready KPIs ===
queries = [

    ("total_transactions", """
        SELECT 
            'Total Transactions' AS "Metric",
            toString(COUNT(*)) AS "Value"
        FROM mcard_transactions
    """, "Total number of transactions in the system",
       "Total Transactions", "single_value", "single_value,kpi"),

    ("total_spend", """
        SELECT 
            'Total Spend' AS "Metric",
            concat(toString(round(SUM(amount) / 1000000, 2)), ' M') AS "Value"
        FROM mcard_transactions
    """, "Total monetary value of all transactions",
       "Total Spend (Millions)", "single_value", "single_value,kpi"),

    ("avg_ticket_size", """
        SELECT 
            'Avg Ticket Size' AS "Metric",
            toString(round(AVG(amount), 2)) AS "Value"
        FROM mcard_transactions
    """, "Average transaction amount",
       "Average Ticket Size", "single_value", "single_value,kpi"),

    ("status_distribution", """
        SELECT 
            transaction_status AS "Status",
            round(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM mcard_transactions), 2) AS "Share (%)"
        FROM mcard_transactions
        GROUP BY transaction_status
        ORDER BY "Share (%)" DESC
    """, "Success vs Failed vs Declined distribution",
       "Transaction Status Breakdown", "pie", "pie,bar"),

    ("channel_usage", """
        SELECT 
            channel AS "Channel",
            round(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM mcard_transactions), 2) AS "Usage Share (%)"
        FROM mcard_transactions
        GROUP BY channel
        ORDER BY "Usage Share (%)" DESC
        LIMIT 6
    """, "Most popular transaction channels",
       "Channel Preference", "bar", "bar,pie"),

    ("top_merchants", """
        SELECT 
            merchant_name AS "Merchant Name",
            round(SUM(amount) / 1000, 1) AS "Spend (K)"
        FROM mcard_transactions
        GROUP BY merchant_name
        ORDER BY "Spend (K)" DESC
        LIMIT 10
    """, "Top 10 merchants by customer spend",
       "Top 10 Merchants Leaderboard", "bar", "bar,table,horizontal_bar"),

    ("segment_distribution", """
        SELECT 
            customer_segment AS "Customer Segment",
            round(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM mcard_transactions), 2) AS "Txn Share (%)"
        FROM mcard_transactions
        GROUP BY customer_segment
        ORDER BY "Txn Share (%)" DESC
        LIMIT 7
    """, "Transaction contribution by customer segment",
       "Segment Contribution", "pie", "pie,bar"),

    ("network_dominance", """
        SELECT 
            card_network AS "Card Network",
            round(SUM(amount) * 100.0 / (SELECT SUM(amount) FROM mcard_transactions), 2) AS "Spend Share (%)"
        FROM mcard_transactions
        GROUP BY card_network
        ORDER BY "Spend Share (%)" DESC
    """, "Which card network dominates spend?",
       "Network Spend Dominance", "Single_value", "Single_value")
]

def run_transaction_portfolio_queries():
    client = initialize_clickhouse_client()
    output_dir = "general_out_csvfiles"
    output_save_dir = "app/general_out_csvfiles"
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(output_save_dir, exist_ok=True)
    results = []

    for i, (name, sql, q_str, desc, chart, suggested_chart) in enumerate(queries, start=1):
        try:
            result = client.query(sql)
            df = pd.DataFrame(result.result_rows, columns=result.column_names[:2])  # Force 2 cols only
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
