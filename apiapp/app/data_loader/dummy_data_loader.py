import random
import pandas as pd
import os
import shutil
import json
from datetime import datetime
import pytz

# Core DB
from app.core.database import Base, engine, default_session

# Models
from app.user.models.user import User
from app.analysis.models.analysis import Analysis
from app.dashboard.models.dashboard import Dashboard
from app.query.models.query import Query
from app.queryresult.models.queryresult import QueryResult
from app.context_table.models.context_table import ContextTable, ContextNavigationMap
from app.entity.model import Entity, EntityContextMap, EntityUserMap
from app.navigation.model import Navigation, NavigationQueryMap
from app.general_query.model import GeneralQuery
from app.auth.utils.auth_utils import get_password_hash

# General Query Runners - Freddie Mac Performance (context_id=1, 10 modules)
from app.general_query.generate_csv_files.fmac_performance.gencsv_custom_operational_metrics import run_custom_operational_metrics_queries
from app.general_query.generate_csv_files.fmac_performance.gencsv_delinquency_analytics import run_delinquency_analytics_queries
from app.general_query.generate_csv_files.fmac_performance.gencsv_expense_cost_analysis import run_expense_cost_analysis_queries
from app.general_query.generate_csv_files.fmac_performance.gencsv_loan_modification_assistance import run_loan_modification_assistance_queries
from app.general_query.generate_csv_files.fmac_performance.gencsv_loan_portfolio_overview import run_loan_portfolio_overview_queries
from app.general_query.generate_csv_files.fmac_performance.gencsv_loss_recovery_analysis import run_loss_recovery_analysis_queries
from app.general_query.generate_csv_files.fmac_performance.gencsv_risk_exposure_analytics import run_risk_exposure_analytics_queries
from app.general_query.generate_csv_files.fmac_performance.gencsv_time_series import run_time_series_queries
from app.general_query.generate_csv_files.fmac_performance.gencsv_vintage_analysis import run_vintage_analysis_queries
from app.general_query.generate_csv_files.fmac_performance.gencsv_zero_balance_loan_resolution_analysis import run_zero_balance_loan_resolution_analysis_queries

# General Query Runners - Freddie Mac Origination (context_id=2, 10 modules)
from app.general_query.generate_csv_files.fmac_origination.gencsv_property_type import run_property_type_queries
from app.general_query.generate_csv_files.fmac_origination.gencsv_loan_purpose import run_loan_purpose_queries
from app.general_query.generate_csv_files.fmac_origination.gencsv_first_payment_year import run_first_payment_year_queries
from app.general_query.generate_csv_files.fmac_origination.gencsv_maturity_year import run_maturity_year_queries
from app.general_query.generate_csv_files.fmac_origination.gencsv_loan_age_wise import run_loan_age_queries
from app.general_query.generate_csv_files.fmac_origination.gencsv_delinquency_status import run_delinquency_status_queries
from app.general_query.generate_csv_files.fmac_origination.gencsv_step_modification_loan_status import run_step_modification_queries
from app.general_query.generate_csv_files.fmac_origination.gencsv_risk_segment import run_risk_segment_queries
from app.general_query.generate_csv_files.fmac_origination.gencsv_temporal_trend_queries import run_temporal_trend_queries
from app.general_query.generate_csv_files.fmac_origination.gencsv_combined_kpi_dashboards import run_combined_kpi_queries

# General Query Runners - Mastercard Transaction (context_id=3, 5 modules)
from app.general_query.generate_csv_files.mcard_transactions.mcard_network_comparison_analytics import run_card_network_queries
from app.general_query.generate_csv_files.mcard_transactions.mcard_customer_profile_segmentation_analytics import run_customer_profile_queries
from app.general_query.generate_csv_files.mcard_transactions.mcard_merchant_spend_performance_analytics import run_merchant_spend_queries
from app.general_query.generate_csv_files.mcard_transactions.mcard_transaction_time_series_trend_analytics import run_transaction_time_series_queries
from app.general_query.generate_csv_files.mcard_transactions.mcard_transaction import run_transaction_portfolio_queries

# ----------------------------------------------------------------------
# CONFIG
# ----------------------------------------------------------------------
number_of_users: int = 7  # 1 SUPERADMIN + 3 Freddie Mac + 3 Mastercard
number_of_analyses: int = 60  # 50 Freddie + 10 Mastercard

# General Query Modules
GENERAL_QUERY_MODULES = [
    # Freddie Mac Performance (context_id=1, 10 navigations)
    ("custom_operational_metrics", run_custom_operational_metrics_queries, "Custom Operational Metrics", 1),
    ("delinquency_analytics", run_delinquency_analytics_queries, "Delinquency Analytics", 1),
    ("expense_cost_analysis", run_expense_cost_analysis_queries, "Expense & Cost Analysis", 1),
    ("loan_modification_assistance", run_loan_modification_assistance_queries, "Loan Modification & Assistance", 1),
    ("loan_portfolio_overview", run_loan_portfolio_overview_queries, "Loan Portfolio Overview", 1),
    ("loss_recovery_analysis", run_loss_recovery_analysis_queries, "Loss & Recovery Analysis", 1),
    ("risk_exposure_analytics", run_risk_exposure_analytics_queries, "Risk & Exposure Analytics", 1),
    ("time_series_performance", run_time_series_queries, "Time Series", 1),
    ("vintage_analysis", run_vintage_analysis_queries, "Vintage Analysis", 1),
    ("zero_balance_loan_resolution", run_zero_balance_loan_resolution_analysis_queries, "Zero Balance / Loan Resolution Analysis", 1),
    # Freddie Mac Origination (context_id=2, 10 navigations)
    ("property_type", run_property_type_queries, "Property Type Analysis", 2),
    ("loan_purpose", run_loan_purpose_queries, "Loan Purpose Analysis", 2),
    ("first_payment_year", run_first_payment_year_queries, "First Payment Year Analysis", 2),
    ("maturity_year", run_maturity_year_queries, "Maturity Year Analysis", 2),
    ("loan_age", run_loan_age_queries, "Loan Age Analysis", 2),
    ("delinquency_status", run_delinquency_status_queries, "Delinquency Status Analysis", 2),
    ("step_modification_loan_status", run_step_modification_queries, "Step Modification & Loan Status Analysis", 2),
    ("risk_segment", run_risk_segment_queries, "Risk Segment Analysis", 2),
    ("temporal_trend", run_temporal_trend_queries, "Temporal Trend Analysis", 2),
    ("combined_kpi_dashboards", run_combined_kpi_queries, "Combined KPI Dashboards", 2),
    # Mastercard Transaction (context_id=3, 5 navigations)
    ("card_network_comparison", run_card_network_queries, "Card Network Comparison", 3),
    ("customer_profile_segmentation", run_customer_profile_queries, "Customer Profile & Segmentation", 3),
    ("merchant_spend_performance", run_merchant_spend_queries, "Merchant Spend & Performance", 3),
    ("transaction_time_series", run_transaction_time_series_queries, "Transaction Time Series Trend", 3),
    ("transaction_portfolio", run_transaction_portfolio_queries, "Transaction Portfolio Overview", 3),
]

# Timezone
india_tz = pytz.timezone('Asia/Kolkata')

# ----------------------------------------------------------------------
# DUMMY DATA ARRAYS (GenAI Queries)
# ----------------------------------------------------------------------
# Freddie Mac
performance_nl_array = [
    "Show performance records where current loan delinquency status is not 0",
    "What is the average current actual UPB from performance?",
    "How many records are in the performance table?",
    "Count of performance records grouped by month",
    "List current interest rates for the first 10 performance records"
]
origination_nl_array = [
    "Show final loan statuses where zero balance code is 01",
    "What is the average original UPB from final status?",
    "How many final loan records are there?",
    "Count of final statuses grouped by zero balance removal date",
    "List current actual UPB for the first 10 final records"
]

performance_summary_array = [
    "5% delinquency rate, mostly 30-day.",
    "Avg current UPB: $210K.",
    "14M+ performance records over 10+ years.",
    "Peak activity in December.",
    "Rates from 3.5% to 5.2%."
]
origination_summary_array = [
    "40% prepayments.",
    "Avg UPB at termination: $215K.",
    "800K loans closed.",
    "Most removals in 2022–2024.",
    "Final UPB avg $50K."
]

performance_chart_array = ["bar", "line", "single_value", "bar", "line"]
origination_chart_array = ["bar", "line", "single_value", "bar", "line"]
performance_suggestedchart_array = ["bar,line", "line,bar", "single_value", "bar,pie", "line,pie"]
origination_suggestedchart_array = ["bar,line", "line,bar", "single_value", "bar,pie", "line,pie"]

# Mastercard (dummy arrays)
mastercard_nl_array = [
    "Show transactions by card network",
    "What is the total spend from Visa cards?",
    "How many transactions in the last month?",
    "Count of transactions grouped by merchant category",
    "List top 10 merchants by spend"
]
mastercard_summary_array = [
    "Visa dominates with 60% volume.",
    "Total spend: $1.2B.",
    "2M+ transactions over 12 months.",
    "Peak in Q4.",
    "Avg ticket: $45."
]
mastercard_chart_array = ["bar", "line", "single_value", "bar", "line"]
mastercard_suggestedchart_array = ["bar,line", "line,bar", "single_value", "bar,pie", "line,pie"]

# ----------------------------------------------------------------------
# SCHEMAS
# ----------------------------------------------------------------------
PERFORMANCE_SCHEMA = {
    "loan_sequence_number": "Unique identifier assigned to each mortgage loan.",
    "monthly_reporting_period": "Month and year the loan performance data was reported.",
    "current_actual_upb": "Current unpaid principal balance of the loan.",
    "current_loan_delinquency_status": "Current delinquency status of the loan in months.",
    "loan_age": "Number of months since the loan's origination.",
    "remaining_months_to_legal_maturity": "Months remaining until the scheduled maturity date.",
    "defect_settlement_date": "Date when any loan defect settlement occurred.",
    "modification_flag": "Indicates whether the loan has been modified.",
    "zero_balance_code": "Code identifying the reason for zero balance (e.g., paid off, foreclosed).",
    "zero_balance_effective_date": "Date when the zero balance became effective.",
    "current_interest_rate": "Current interest rate applicable to the loan.",
    "current_deferred_upb": "Current deferred unpaid principal balance.",
    "ddlpi": "Date of the last paid installment.",
    "mi_recoveries": "Amount recovered from mortgage insurance claims.",
    "net_sale_proceeds": "Net proceeds from property sale after expenses.",
    "non_mi_recoveries": "Recoveries from non-mortgage insurance sources.",
    "expenses": "Total expenses incurred on the loan or property.",
    "legal_costs": "Legal expenses related to loan servicing or foreclosure.",
    "maintenance_preservation_costs": "Costs for maintaining or preserving the property.",
    "taxes_insurance": "Taxes and insurance payments associated with the loan.",
    "misc_expenses": "Miscellaneous loan-related expenses.",
    "actual_loss": "Actual loss incurred on the loan after liquidation or modification.",
    "modification_cost": "Cost associated with modifying the loan terms.",
    "step_modification_flag": "Indicates whether the loan has a step-rate modification.",
    "deferred_payment_plan": "Shows if the loan has a deferred payment plan.",
    "estimated_ltv": "Estimated loan-to-value ratio based on current valuation.",
    "zero_balance_removal_upb": "Unpaid principal balance at the time of zero balance removal.",
    "delinquent_accrued_interest": "Interest accrued while the loan was delinquent.",
    "delinquency_due_to_disaster": "Flag indicating if delinquency is due to a disaster event.",
    "borrower_assistance_status_code": "Code indicating borrower assistance or workout status.",
    "current_month_modification_cost": "Modification cost recognized in the current month.",
    "interest_bearing_upb": "Portion of UPB that continues to bear interest."
}

ORIGINATION_SCHEMA = {
    "loan_sequence_number": "Unique identifier assigned to each mortgage loan at origination.",
    "original_upb": "Original unpaid principal balance of the loan.",
    "original_interest_rate": "Interest rate at the time of loan origination.",
    "maturity_date": "Scheduled maturity date of the loan.",
    "original_dti_ratio": "Original debt-to-income ratio of the borrower.",
    "original_ltv": "Original loan-to-value ratio at origination.",
    "ppm_flag": "Indicator if the loan includes a prepayment penalty.",
    "current_actual_upb": "Current unpaid principal balance of the loan.",
    "actual_loss": "Actual loss recognized on the loan after resolution.",
    "current_loan_delinquency_status": "Current delinquency status of the loan in months.",
    "remaining_months_to_legal_maturity": "Months remaining until loan maturity.",
    "zero_balance_code": "Reason code for zero balance (paid off, foreclosure, etc.).",
    "loan_age": "Number of months since the loan's origination.",
    "monthly_reporting_period": "Month and year of data reporting.",
    "first_payment_year": "Year of the first scheduled loan payment.",
    "first_payment_month": "Month of the first scheduled loan payment.",
    "maturity_year": "Year of the loan's scheduled maturity.",
    "maturity_month": "Month of the loan's scheduled maturity.",
    "property_state": "Two-letter state code where the property is located.",
    "property_type": "Type of property securing the loan (e.g., SF, condo).",
    "loan_purpose": "Purpose of the loan (purchase, refinance, etc.).",
    "legal_costs": "Legal fees related to the loan.",
    "maintenance_preservation_costs": "Expenses for maintaining or preserving the property.",
    "taxes_insurance": "Taxes and insurance costs related to the property.",
    "misc_expenses": "Other miscellaneous loan-related expenses.",
    "step_modification_flag": "Indicates whether the loan has a step-rate modification.",
    "loan_status": "Current loan performance status (e.g., active, liquidated)."
}

TRANSACTION_SCHEMA = {
    "customer_id": "Unique identifier for the customer performing the transaction.",
    "transaction_date": "Date and time when the transaction occurred.",
    "amount": "Monetary value of the transaction.",
    "merchant_category_code": "Code representing the type of merchant or industry (MCC).",
    "customer_city": "City where the customer is located.",
    "card_network": "The card network used (e.g., Visa, Mastercard).",
    "transaction_status": "Status of the transaction (e.g., success, failed, declined).",
    "channel": "Transaction channel (e.g., POS, ATM, ONLINE).",
    "card_type": "Type of card (e.g., debit, credit, prepaid).",
    "merchant_name": "Name of the merchant where the transaction took place.",
    "customer_segment": "Segment or category the customer belongs to (e.g., retail, corporate)."
}

# ----------------------------------------------------------------------
# LOADERS
# ----------------------------------------------------------------------
def load_user_data(num_records: int = 7):
    try:
        session = default_session()
        if session.query(User).count() == 0:
            # User 1: SUPERADMIN
            superadmin = User(
                name="SuperAdmin",
                email="superadmin@global.com",
                password=get_password_hash("Super*2025"),
                role="SUPERADMIN",
                is_active=True
            )
            session.add(superadmin)
            session.flush()

            # Freddie Mac Users (2-4)
            freddie_users = [
                ("FreddieAdmin", "freddie.admin@freddie.com", "ADMIN"),
                ("FreddieUser1", "freddie.user1@freddie.com", "USER"),
                ("FreddieUser2", "freddie.user2@freddie.com", "USER")
            ]
            for name, email, role in freddie_users:
                session.add(User(
                    name=name,
                    email=email,
                    password=get_password_hash(f"{name}*2025"),
                    role=role,
                    is_active=True
                ))

            # Mastercard Users (5-7)
            mastercard_users = [
                ("MastercardAdmin", "mastercard.admin@mastercard.com", "ADMIN"),
                ("MastercardUser1", "mastercard.user1@mastercard.com", "USER"),
                ("MastercardUser2", "mastercard.user2@mastercard.com", "USER")
            ]
            for name, email, role in mastercard_users:
                session.add(User(
                    name=name,
                    email=email,
                    password=get_password_hash(f"{name}*2025"),
                    role=role,
                    is_active=True
                ))

            session.commit()
            print(f"Loaded {num_records} users: 1 SUPERADMIN, 3 Freddie Mac, 3 Mastercard.")
        else:
            print("Users already exist.")
    except Exception as e:
        session.rollback()
        print(f"Error loading users: {e}")
    finally:
        session.close()

def load_entity_data():
    try:
        session = default_session()
        if session.query(Entity).count() == 0:
            entities = [
                {"name": "Freddie Mac", "description": "Freddie Mac mortgage data"},
                {"name": "Mastercard", "description": "Mastercard transaction data"}
            ]
            for e in entities:
                session.add(Entity(name=e["name"], description=e["description"]))
            session.commit()
            print(f"Loaded {len(entities)} entities.")
        else:
            print("Entities already exist.")
    except Exception as e:
        session.rollback()
        print(f"Error loading entities: {e}")
    finally:
        session.close()

def load_analysis_data(num_records: int = 60):
    try:
        session = default_session()
        if session.query(Analysis).count() == 0:
            # Freddie Mac analyses (50)
            freddie_user_ids = [2, 3, 4]
            for i in range(50):
                user_id = random.choice(freddie_user_ids)
                session.add(Analysis(title=f"Freddie Analysis {i + 1}", user_id=user_id))
            
            # Mastercard analyses (10)
            mastercard_user_ids = [5, 6, 7]
            for i in range(10):
                user_id = random.choice(mastercard_user_ids)
                session.add(Analysis(title=f"Mastercard Analysis {i + 1}", user_id=user_id))
            
            session.commit()
            print(f"Loaded {num_records} analyses: 50 Freddie Mac, 10 Mastercard.")
        else:
            print("Analyses already exist.")
    except Exception as e:
        session.rollback()
        print(f"Error loading analyses: {e}")
    finally:
        session.close()

def load_dashboard_data():
    try:
        session = default_session()
        if session.query(Dashboard).count() == 0:
            analyses = session.query(Analysis).all()
            dashboard_count = 0

            # First 5 Mastercard analyses → 2 dashboards each
            mastercard_analyses = [a for a in analyses if "Mastercard" in a.title]
            first_5 = mastercard_analyses[:5]
            for analysis in first_5:
                for _ in range(2):
                    dashboard_count += 1
                    session.add(Dashboard(title=f"MC Dashboard {dashboard_count}", analysis_id=analysis.analysis_id))

            # Next 5 Mastercard analyses → 1 dashboard each
            next_5 = mastercard_analyses[5:10]
            for analysis in next_5:
                dashboard_count += 1
                session.add(Dashboard(title=f"MC Dashboard {dashboard_count}", analysis_id=analysis.analysis_id))

            # Freddie Mac dashboards (random 1-2 per analysis)
            freddie_analyses = [a for a in analyses if "Freddie" in a.title]
            for analysis in freddie_analyses:
                for _ in range(random.randint(1, 2)):
                    dashboard_count += 1
                    session.add(Dashboard(title=f"Freddie Dashboard {dashboard_count}", analysis_id=analysis.analysis_id))

            session.commit()
            print(f"Loaded {dashboard_count} dashboards (15 Mastercard + rest Freddie).")
        else:
            print("Dashboards already exist.")
    except Exception as e:
        session.rollback()
        print(f"Error loading dashboards: {e}")
    finally:
        session.close()

def load_context_table_data():
    try:
        session = default_session()
        if session.query(ContextTable).count() == 0:
            contexts = [
                ("Performance", json.dumps(PERFORMANCE_SCHEMA), "loan_sequence_number: string\ncurrent_actual_upb: double\n..."),
                ("Origination", json.dumps(ORIGINATION_SCHEMA), "loan_sequence_number: string\nzero_balance_code: string\n..."),
                ("Transactions", json.dumps(TRANSACTION_SCHEMA), "customer_id: string\ntransaction_date: datetime\namount: double\n...")
            ]
            for name, schema_info, llm_schema in contexts:
                session.add(ContextTable(name=name, schema_info=schema_info, llm_schema=llm_schema))
            session.commit()
            print("Loaded 3 context tables: 2 Freddie Mac, 1 Mastercard.")
        else:
            print("Context tables already exist.")
    except Exception as e:
        session.rollback()
        print(f"Error loading context tables: {e}")
    finally:
        session.close()

def load_entity_context_map_data():
    try:
        session = default_session()
        if session.query(EntityContextMap).count() == 0:
            freddie = session.query(Entity).filter(Entity.name == "Freddie Mac").first()
            mastercard = session.query(Entity).filter(Entity.name == "Mastercard").first()
            contexts = session.query(ContextTable).all()
            
            # Freddie Mac: contexts 1-2
            for ctx in contexts[:2]:
                session.add(EntityContextMap(entity_id=freddie.entity_id, context_id=ctx.context_id))
            
            # Mastercard: context 3
            session.add(EntityContextMap(entity_id=mastercard.entity_id, context_id=contexts[2].context_id))
            
            session.commit()
            print("Linked Freddie Mac to 2 contexts, Mastercard to 1 context.")
        else:
            print("EntityContextMap already exists.")
    except Exception as e:
        session.rollback()
        print(f"Error: {e}")
    finally:
        session.close()

def load_entity_user_map_data():
    try:
        session = default_session()
        if session.query(EntityUserMap).count() == 0:
            superadmin = session.query(User).filter(User.role == "SUPERADMIN").first()
            freddie = session.query(Entity).filter(Entity.name == "Freddie Mac").first()
            mastercard = session.query(Entity).filter(Entity.name == "Mastercard").first()

            # SUPERADMIN -> both
            session.add(EntityUserMap(user_id=superadmin.user_id, entity_id=freddie.entity_id))
            session.add(EntityUserMap(user_id=superadmin.user_id, entity_id=mastercard.entity_id))

            # Freddie Mac users (2-4)
            freddie_users = session.query(User).filter(User.user_id.in_([2, 3, 4])).all()
            for user in freddie_users:
                session.add(EntityUserMap(user_id=user.user_id, entity_id=freddie.entity_id))

            # Mastercard users (5-7)
            mastercard_users = session.query(User).filter(User.user_id.in_([5, 6, 7])).all()
            for user in mastercard_users:
                session.add(EntityUserMap(user_id=user.user_id, entity_id=mastercard.entity_id))

            session.commit()
            print("Loaded EntityUserMap: SUPERADMIN (both), Freddie (3), Mastercard (3).")
        else:
            print("EntityUserMap already exists.")
    except Exception as e:
        session.rollback()
        print(f"Error: {e}")
    finally:
        session.close()

def load_query_and_result_data():
    try:
        session = default_session()
        if session.query(Query).count() == 0:
            analyses = session.query(Analysis).all()
            query_count = 0
            general_csv_dir = "app/general_out_csvfiles"

            for analysis in analyses:
                dashboards = session.query(Dashboard).filter(Dashboard.analysis_id == analysis.analysis_id).all()
                for dashboard in dashboards:
                    for _ in range(5):
                        user_id = analysis.user_id
                        if "Freddie" in analysis.title:
                            context_id = random.choice([1, 2])
                            if context_id == 1:
                                nl_arr = performance_nl_array
                                sum_arr = performance_summary_array
                                chart_arr = performance_chart_array
                                sug_arr = performance_suggestedchart_array
                                prefix = "performance"
                            else:
                                nl_arr = origination_nl_array
                                sum_arr = origination_summary_array
                                chart_arr = origination_chart_array
                                sug_arr = origination_suggestedchart_array
                                prefix = "origination"
                        else:  # Mastercard
                            context_id = 3
                            nl_arr = mastercard_nl_array
                            sum_arr = mastercard_summary_array
                            chart_arr = mastercard_chart_array
                            sug_arr = mastercard_suggestedchart_array
                            prefix = "mastercard"

                        idx = random.randint(0, 4)
                        query = Query(
                            query_str=nl_arr[idx],
                            analysis_id=analysis.analysis_id,
                            context_id=context_id,
                            dashboard_id=dashboard.dashboard_id,
                            created_at=datetime.now(india_tz),
                            updated_at=datetime.now(india_tz)
                        )
                        session.add(query)
                        session.flush()
                        query_count += 1

                        csv_name = f"query{query.query_id}_user{user_id}_analysis{analysis.analysis_id}_result.csv"
                        csv_path = os.path.join("../out_csvfiles", csv_name)

                        result = QueryResult(
                            csv_file_name=csv_path,
                            chart_type=chart_arr[idx],
                            suggested_charts=sug_arr[idx],
                            summary=sum_arr[idx],
                            query_id=query.query_id
                        )
                        session.add(result)
                        session.commit()

                        # Copy from general analytics CSV (if exists)
                        general_csv_name = f"general_query_{idx + 1}_{prefix}.csv"
                        src = os.path.join(general_csv_dir, general_csv_name)
                        dst = os.path.join(os.path.dirname(__file__), "../out_csvfiles", csv_name)
                        os.makedirs(os.path.dirname(dst), exist_ok=True)

                        if os.path.exists(src):
                            shutil.copy2(src, dst)
                            print(f"Copied {src} → {dst}")
                        else:
                            # Fallback dummy
                            pd.DataFrame({'result': [random.randint(100, 1000)]}).to_csv(dst, index=False)

            print(f"Loaded {query_count} queries + results (75 for Mastercard).")
        else:
            print("Queries already exist.")
    except Exception as e:
        session.rollback()
        print(f"Error loading queries: {e}")
    finally:
        session.close()

def load_general_query_modules():
    try:
        session = default_session()
        for folder, runner_func, nav_title, context_id in GENERAL_QUERY_MODULES:
            nav = session.query(Navigation).filter(Navigation.navigation_title == nav_title).first()
            if not nav:
                nav = Navigation(
                    navigation_title=nav_title,
                    navigation_stem=f"/{folder.replace('_', '-')}",
                    navigation_description=f"Analysis for {nav_title.lower()}"
                )
                session.add(nav)
                session.commit()
                session.refresh(nav)
                print(f"Created navigation: {nav_title}")

            existing_cnav = session.query(ContextNavigationMap).filter(
                ContextNavigationMap.context_id == context_id,
                ContextNavigationMap.navigation_id == nav.navigation_id
            ).first()
            if not existing_cnav:
                session.add(ContextNavigationMap(context_id=context_id, navigation_id=nav.navigation_id))
                session.commit()

            query_results = runner_func()
            for result in query_results:
                gen_query = GeneralQuery(
                    query_sql_string=result["query_sql_string"],
                    query_str=result["query_str"],
                    description=result["description"],
                    csv_file_path=result["csv_file_path"],
                    chart_type=result["chart_type"],
                    suggested_chart_types=result["suggested_chart_types"],
                    summary=""
                )
                session.add(gen_query)
                session.flush()
                session.add(NavigationQueryMap(navigation_id=nav.navigation_id, query_id=gen_query.query_id))
            session.commit()
            print(f"Loaded {len(query_results)} general queries for {nav_title}.")
    except Exception as e:
        session.rollback()
        print(f"Error loading general query modules: {e}")
    finally:
        session.close()

# ----------------------------------------------------------------------
# MAIN LOADER
# ----------------------------------------------------------------------
def load_dummy_data(db_engine):
    global engine
    engine = db_engine

    Base.metadata.create_all(bind=engine, checkfirst=True)
    print("All tables created.")

    load_user_data(number_of_users)
    load_entity_data()
    load_analysis_data(number_of_analyses)
    load_dashboard_data()
    load_context_table_data()
    load_entity_context_map_data()
    load_entity_user_map_data()
    load_query_and_result_data()
    load_general_query_modules()

    print("Dummy data load complete.")

# ----------------------------------------------------------------------
# RUN DIRECTLY
# ----------------------------------------------------------------------
if __name__ == "__main__":
    Base.metadata.drop_all(bind=engine)
    load_dummy_data(engine)
