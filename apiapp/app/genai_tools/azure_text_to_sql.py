import os
import re
import sys
import clickhouse_connect
import pandas as pd
from openai import OpenAI

# ==============================
# 1. AZURE OPENAI (Phi-4) CONFIG
# ==============================
endpoint = "https://genaimodelserver.services.ai.azure.com/openai/v1/"
deployment_name = "Phi-4"
api_key = "EZmqLicO0FLxaDtQtBC3j8IjiEpqzcL3upHm6lHKuBeqNRJ6eK2ZJQQJ99BJACYeBjFXJ3w3AAAAACOGWBJD"

azure_client = OpenAI(
    base_url=endpoint,
    api_key=api_key
)

SQL_FENCE_RE = re.compile(r"```sql\s*(.*?)\s*```", re.DOTALL | re.IGNORECASE)

# ==============================
# 2. SCHEMAS & TABLES
# ==============================
PERFORMANCE_SCHEMA = """
loan_sequence_number: string
monthly_reporting_period: date
current_actual_upb: double
current_loan_delinquency_status: string
loan_age: int
remaining_months_to_legal_maturity: int
defect_settlement_date: string
modification_flag: string
zero_balance_code: string
zero_balance_effective_date: string
current_interest_rate: float
current_deferred_upb: double
ddlpi: string
mi_recoveries: double
net_sale_proceeds: string
non_mi_recoveries: double
expenses: double
legal_costs: double
maintenance_preservation_costs: double
taxes_insurance: double
misc_expenses: double
actual_loss: double
modification_cost: double
step_modification_flag: string
deferred_payment_plan: string
estimated_ltv: int
zero_balance_removal_upb: double
delinquent_accrued_interest: double
delinquency_due_to_disaster: string
borrower_assistance_status_code: string
current_month_modification_cost: double
interest_bearing_upb: double
"""

ORIGINATION_SCHEMA = """
loan_sequence_number: string
original_upb: double
original_interest_rate: double
maturity_date: date
original_dti_ratio: int
original_ltv: int
ppm_flag: string
current_actual_upb: double
actual_loss: double
current_loan_delinquency_status: string
remaining_months_to_legal_maturity: int
zero_balance_code: string
loan_age: int
monthly_reporting_period: date
first_payment_year: int
first_payment_month: int
maturity_year: int
maturity_month: int
property_state: string
property_type: string
loan_purpose: string
legal_costs: double
maintenance_preservation_costs: double
taxes_insurance: double
misc_expenses: double
step_modification_flag: string
loan_status: string
"""

# ==============================
# Transactions Schema
# ==============================
TRANSACTIONS_SCHEMA = """
transaction_id: string
transaction_date: date
amount: double
currency: string
merchant_category_code: string
merchant_name: string
merchant_city: string
terminal_id: string
channel: string
transaction_status: string
card_number_masked: string
card_type: string
card_network: string
customer_id: string
customer_age_group: string
customer_gender: string
customer_city: string
customer_segment: string
"""

TABLES = {
    "performance": {"table": "performance_updated_2025_ctas", "schema": PERFORMANCE_SCHEMA},
    "origination": {"table": "final_loan_status_proj", "schema": ORIGINATION_SCHEMA},
    "transactions": {"table": "mcard_transactions", "schema": TRANSACTIONS_SCHEMA},
}

# ==============================
# 3. CLICKHOUSE CONFIG
# ==============================
CLICKHOUSE_CONFIG = {
    "host": "57.159.27.80",
    "port": 8123,
    "username": "fmacadmin",
    "password": "fmac*2025",
    "database": "fmac_db",
}

def initialize_clickhouse_client():
    return clickhouse_connect.get_client(**CLICKHOUSE_CONFIG)

# ==============================
# 4. HELPERS
# ==============================
def parse_schema_columns(schema_text: str) -> set:
    cols = set()
    for line in schema_text.strip().splitlines():
        if ":" not in line:
            continue
        col = line.split(":", 1)[0].strip().lower()
        cols.add(col)
    return cols

def validate_sql_against_schema(sql: str, schema_cols: set, table_name: str) -> (bool, str):
    if not sql or not isinstance(sql, str):
        return False, "Generated SQL is empty."
    lower_sql = sql.lower()
    if "select" not in lower_sql or "from" not in lower_sql:
        return False, "Missing SELECT or FROM clause."
    if table_name.lower() not in lower_sql:
        return False, f"Expected table `{table_name}` not found in SQL."
    return True, "Valid"

def clean_sql(sql: str) -> str:
    sql = re.sub(r'GROUP BY\s*\*/', '', sql, flags=re.IGNORECASE).strip()
    return sql

# ==============================
# 5. Phi-4: Text → SQL
# ==============================
def genai_text_to_sql(question: str, context: str) -> str:
    table_info = TABLES[context]
    schema = table_info["schema"]
    table = table_info["table"]

    examples = f"""
Examples:

Question: total number of records
SQL:
SELECT COUNT(*) AS total_count
FROM {table};

Question: total transaction amount by city
SQL:
SELECT merchant_city, SUM(amount) AS total_amount
FROM {table}
GROUP BY merchant_city;

Question: average transaction amount by channel
SQL:
SELECT channel, AVG(amount) AS avg_amount
FROM {table}
GROUP BY channel;
"""

    genai_prompt = f"""
You are a SQL expert generating ClickHouse queries based on user questions.

Table: {table}
Schema:
{schema}

Rules:
- Use ONLY the table name and columns provided.
- Include GROUP BY only when needed.
- Use ClickHouse functions (toYear, toMonth, toDate, now, parseDateTimeBestEffort) only if required.
- Output ONLY SQL, no explanations.
- Use the following examples as a reference:
{examples}

User Question: {question}
"""

    completion = azure_client.chat.completions.create(
        model=deployment_name,
        messages=[{"role": "user", "content": genai_prompt}],
    )

    message_content = completion.choices[0].message.content
    match = SQL_FENCE_RE.search(message_content)
    sql_query = match.group(1).strip() if match else message_content.strip()
    sql_query = clean_sql(sql_query)
    return sql_query.rstrip(";")

# ==============================
# 6. Phi-4: Summarize Results + Chart Suggestions
# ==============================
def azure_summarize_and_suggest_charts(df: pd.DataFrame, question: str) -> str:
    if df.empty:
        return "⚠️ No data returned."

    preview = df.head(20).to_markdown(index=False)
    genai_prompt = f"""
The user asked: {question}

Result sample:
{preview}

Task:
1. Write a short 3–4 line summary.
2. Suggest the best chart type (Bar, Line, Pie, Card, etc.).
3. Suggest up to 2 alternative charts.

Format:

### Summary
<text>

### Main Chart
<one word>

### Other Suggested Charts
- <one word>
- <one word>
"""

    completion = azure_client.chat.completions.create(
        model=deployment_name,
        messages=[{"role": "user", "content": genai_prompt}],
    )

    summary_text = completion.choices[0].message.content.strip()
    lines = summary_text.splitlines()
    main_chart_line = next((l for l in lines if l.startswith("### Main Chart")), "")
    main_chart = lines[lines.index(main_chart_line) + 1].strip() if main_chart_line else ""
    if main_chart.lower() == "card":
        summary_text = "\n".join(
            l for l in lines if not l.startswith("### Other") and not l.startswith("-")
        )

    print("\n📈 Analysis & Visualization Suggestions:\n", summary_text)
    return summary_text

# ==============================
# 7. SAMPLE QUESTIONS
# ==============================
SAMPLE_QUESTIONS = {
    "performance": [
        "What is the average current interest rate by year?",
        "How many loans are currently delinquent?",
        "What is the trend of average loan age over time?",
        "What is the distribution of zero balance codes?",
    ],
    "origination": [
        "How many loans have fully matured?",
        "What is the total actual loss by property state?",
        "What is the average current actual UPB by loan purpose?",
        "How many loans are in each loan status category?",
    ],
    "transactions": [
        "What is the total transaction amount by city?",
        "Which merchant categories have the highest total spend?",
        "How many successful transactions occurred last month?",
        "What is the average transaction amount by customer segment?",
        "How many transactions were made per channel?",
        "What is the trend of total amount over time?",
    ],
}

def generate_sample_questions(context: str) -> str:
    return "\n".join(f"- {q}" for q in SAMPLE_QUESTIONS.get(context, []))

# ==============================
# 8. QUERY RUNNER
# ==============================
COUNTER_FILE = "query_counter.txt"

def load_query_counter() -> int:
    if os.path.exists(COUNTER_FILE):
        try:
            return int(open(COUNTER_FILE).read().strip())
        except Exception:
            return 0
    return 0

def save_query_counter(value: int):
    with open(COUNTER_FILE, "w") as f:
        f.write(str(value))

QUERY_COUNTER = load_query_counter()

def get_next_query_id() -> str:
    global QUERY_COUNTER
    QUERY_COUNTER += 1
    save_query_counter(QUERY_COUNTER)
    return f"query{QUERY_COUNTER}"

def run_query_from_prompt(question: str, context: str):
    table_info = TABLES[context]
    schema_cols = parse_schema_columns(table_info["schema"])
    sql_query = genai_text_to_sql(question, context)

    is_valid, msg = validate_sql_against_schema(sql_query, schema_cols, table_info["table"])
    if not is_valid:
        print(f"❌ Invalid query: {msg}\n")
        print("💡 Try these sample questions instead:\n")
        print(generate_sample_questions(context))
        return pd.DataFrame()

    try:
        client = initialize_clickhouse_client()
        print("✅ Connected to ClickHouse")
        result = client.query(sql_query)
        df = pd.DataFrame(result.result_rows, columns=result.column_names)
    except Exception as e:
        print(f"❌ Query execution failed: {e}")
        print("\n💡 Try these sample questions instead:\n")
        print(generate_sample_questions(context))
        return pd.DataFrame()

    print("\n📊 Query Result:")
    print(df.to_string(index=False) if not df.empty else "⚠️ No rows returned.")

    if not df.empty:
        azure_summarize_and_suggest_charts(df, question)

    return df

# ==============================
# 9. MAIN EXECUTION
# ==============================
if __name__ == "__main__":
    context = input("Choose table (origination/performance/transactions): ").strip().lower()
    if context not in TABLES:
        sys.exit("❌ Invalid choice!")
    user_question = input(f"Ask a question about {context}: ")
    run_query_from_prompt(user_question, context)