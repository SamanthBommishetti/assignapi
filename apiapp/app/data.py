from datetime import datetime
from .schemas import Analysis, Query, Dashboard, QueryResult
# -------------------------------
# Analysis Data
# -------------------------------
analysis_data = [
    Analysis(analysisId=1, title="Origination Analysis", timestamp=int(datetime.now().timestamp()), userId=1),
    Analysis(analysisId=2, title="Performance Analysis", timestamp=int(datetime.now().timestamp()), userId=2),
]

# -------------------------------
# Origination Queries
# -------------------------------
origination_query_data = [
    Query(queryId=1, queryStr="What is the total loan count originated by year", analysisId=1, contextId=101, dashboardId=201, resultId=301, timestamp=int(datetime.now().timestamp())),
    Query(queryId=2, queryStr="What is the distribution of loan purpose (purchase, refinance, cash-out refinance)", analysisId=1, contextId=102, dashboardId=202, resultId=302, timestamp=int(datetime.now().timestamp())),
    Query(queryId=3, queryStr="What is the average borrower credit score at origination, by year", analysisId=1, contextId=103, dashboardId=203, resultId=303, timestamp=int(datetime.now().timestamp())),
    Query(queryId=4, queryStr="How does the loan-to-value (LTV) ratio distribution look across loans", analysisId=1, contextId=104, dashboardId=204, resultId=304, timestamp=int(datetime.now().timestamp())),
    Query(queryId=5, queryStr="What is the breakdown of fixed-rate vs adjustable-rate mortgages (ARM)", analysisId=1, contextId=105, dashboardId=205, resultId=305, timestamp=int(datetime.now().timestamp())),
    Query(queryId=6, queryStr="What is the trend of average loan amount over time", analysisId=1, contextId=106, dashboardId=206, resultId=306, timestamp=int(datetime.now().timestamp())),
    Query(queryId=7, queryStr="How do origination volumes vary by state", analysisId=1, contextId=107, dashboardId=207, resultId=307, timestamp=int(datetime.now().timestamp())),
    Query(queryId=8, queryStr="What is the distribution of property types (single-family, condo, multi-unit)", analysisId=1, contextId=108, dashboardId=208, resultId=308, timestamp=int(datetime.now().timestamp())),
    Query(queryId=9, queryStr="How does origination channel (broker vs retail vs correspondent) affect loan characteristics", analysisId=1, contextId=109, dashboardId=209, resultId=309, timestamp=int(datetime.now().timestamp())),
    Query(queryId=10, queryStr="What is the total original unpaid principal balance (UPB) by year", analysisId=1, contextId=110, dashboardId=210, resultId=310, timestamp=int(datetime.now().timestamp())),
    Query(queryId=11, queryStr="What is the average original debt-to-income ratio (original_dti_ratio) by year", analysisId=1, contextId=111, dashboardId=211, resultId=311, timestamp=int(datetime.now().timestamp())),
    Query(queryId=12, queryStr="How many first-time homebuyers (first_time_homebuyer_flag) were there each year", analysisId=1, contextId=112, dashboardId=212, resultId=312, timestamp=int(datetime.now().timestamp())),
    Query(queryId=13, queryStr="What is the distribution of loans by number of units (number_of_units)", analysisId=1, contextId=113, dashboardId=213, resultId=313, timestamp=int(datetime.now().timestamp())),
    Query(queryId=14, queryStr="What is the trend of average mortgage insurance percentage (mi_pct) by year", analysisId=1, contextId=114, dashboardId=214, resultId=314, timestamp=int(datetime.now().timestamp())),
    Query(queryId=15, queryStr="What is the breakdown of loans by occupancy status (occupancy_status)", analysisId=1, contextId=115, dashboardId=215, resultId=315, timestamp=int(datetime.now().timestamp())),
    Query(queryId=16, queryStr="How many loans were originated through each channel (channel)", analysisId=1, contextId=116, dashboardId=216, resultId=316, timestamp=int(datetime.now().timestamp())),
    Query(queryId=17, queryStr="What are the top 10 sellers (seller_name) by total loan count", analysisId=1, contextId=117, dashboardId=217, resultId=317, timestamp=int(datetime.now().timestamp())),
    Query(queryId=18, queryStr="How does the average original interest rate (original_interest_rate) differ by property type (property_type)", analysisId=1, contextId=118, dashboardId=218, resultId=318, timestamp=int(datetime.now().timestamp())),
    Query(queryId=19, queryStr="What is the trend of interest-only loans (interest_only_indicator) over time", analysisId=1, contextId=119, dashboardId=219, resultId=319, timestamp=int(datetime.now().timestamp())),
    Query(queryId=20, queryStr="What is the Loan count by Property State", analysisId=1, contextId=120, dashboardId=220, resultId=320, timestamp=int(datetime.now().timestamp())),
]

# -------------------------------
# Performance Queries
# -------------------------------
performance_query_data = [
    Query(queryId=21, queryStr="How many loans have a modification_flag = 'Y'?", analysisId=2, contextId=201, dashboardId=301, resultId=401, timestamp=int(datetime.now().timestamp())),
    Query(queryId=22, queryStr="What is the total modification cost (modification_cost) by year", analysisId=2, contextId=202, dashboardId=302, resultId=402, timestamp=int(datetime.now().timestamp())),
    Query(queryId=23, queryStr="What is the total actual_loss across all loans", analysisId=2, contextId=203, dashboardId=303, resultId=403, timestamp=int(datetime.now().timestamp())),
    Query(queryId=24, queryStr="What is the total zero_balance_removal_upb for loans that reached zero balance", analysisId=2, contextId=204, dashboardId=304, resultId=404, timestamp=int(datetime.now().timestamp())),
    Query(queryId=25, queryStr="How many loans have interest-bearing UPB vs deferred UPB components", analysisId=2, contextId=205, dashboardId=305, resultId=405, timestamp=int(datetime.now().timestamp())),
    Query(queryId=26, queryStr="How many loans are in borrower assistance programs", analysisId=2, contextId=206, dashboardId=306, resultId=406, timestamp=int(datetime.now().timestamp())),
    Query(queryId=27, queryStr="What are the top drivers of expenses (legal_costs, maintenance_preservation_costs, taxes_insurance, misc_expenses)", analysisId=2, contextId=207, dashboardId=307, resultId=407, timestamp=int(datetime.now().timestamp())),
    Query(queryId=28, queryStr="How many loans became delinquent due to disaster (delinquency_due_to_disaster = 'Y')", analysisId=2, contextId=208, dashboardId=308, resultId=408, timestamp=int(datetime.now().timestamp())),
    Query(queryId=29, queryStr="Deferred UPB balances trend", analysisId=2, contextId=209, dashboardId=309, resultId=409, timestamp=int(datetime.now().timestamp())),
    Query(queryId=30, queryStr="Loans paid off (zero_balance_code) by year", analysisId=2, contextId=210, dashboardId=310, resultId=410, timestamp=int(datetime.now().timestamp())),
]

# -------------------------------
# Dashboard Data
# -------------------------------
dashboard_data = [
    Dashboard(dashboardId=201, title="Origination Dashboard", analysisId=1, timestamp=int(datetime.now().timestamp())),
    Dashboard(dashboardId=301, title="Performance Dashboard", analysisId=2, timestamp=int(datetime.now().timestamp())),
]

# -------------------------------
# Result Data (examples only)
# -------------------------------
result_data = [
    QueryResult(resultId=301, queryId=1, csvFileName="origination_loan_count.csv", chartType="bar", summary="Total loans originated by year"),
    QueryResult(resultId=401, queryId=21, csvFileName="loan_modifications.csv", chartType="line", summary="Loans with modification_flag = Y"),
]
