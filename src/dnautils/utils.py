"""  FUNCTIONS FOR DATABASE CONNECTIONS
LOAD LOCALIZATION.R FOR CREDENTIALS TO ACCESS DB """

"""
Created on Thu Apr 13 15:07:05 2023

@author: unknown
@lastupdate: vidy Fri Sept 15 2023
@lastupdate: kevin Thu Nov 16 2023
@lastudpate: vidy Mon Mar 24 2025
"""


# Loading Libraries

import pandas as pd
import sqlalchemy as sa
from sqlalchemy.engine.url import URL
from snowflake.sqlalchemy import URL as sURL
import getpass
import os
import platform
import re
import sys
from .pykeys import *
from snowflake.snowpark import Session

from cryptography.hazmat.primitives import serialization

# For reading local config

script_path = f'/Users/{os.environ["USER"]}/Data/config.txt'
exec(open(script_path).read())

# Server side config
sysname = platform.uname()[0]

if platform.system() == "Linux":
    print("Server")
    credential_path = "/home/ec2-user/pa-lead/creative_studio/python"
elif getpass.getuser() == "jan":
    print("Local - Jan")
    credential_path = "/Users/jan/Documents/Reporting/script"

# Enable SSL for all Redshift connections made via psycopg2
os.environ["REDSHIFT_SSLMODE"] = "disable"


def RS_Adv_conn_func():
    adv_engine_string = URL.create(
        drivername="redshift+psycopg2",
        database="stackadaptdev",
        host="127.0.0.1",
        port=20200,
    )
    # global adv_engine
    adv_engine = sa.create_engine(adv_engine_string, connect_args={"sslmode": "disable"})
    return adv_engine


def RS_Daily_conn_func(user_id):
    mod_value = user_id % 4
    print(f"Mod Value : {mod_value}")
    if mod_value == 0:
        if sysname == "Linux":
            dl_engine_string = URL.create(
                drivername="redshift+psycopg2",
                database="stackadaptdev",
                host=redshift_dl_host_0,
                port=dl_port,
                username=rs_user,
                password=rs_password,
            )
            dl_engine = sa.create_engine(dl_engine_string, connect_args={"sslmode": "disable"})
        else:
            dl_engine_string = URL.create(
                drivername="redshift+psycopg2",
                database="stackadaptdev",
                host="127.0.0.1",
                port=16441,
            )
            dl_engine = sa.create_engine(dl_engine_string, connect_args={"sslmode": "disable"})
            print("Daily Cluster: 0")
    elif mod_value == 1:
        if sysname == "Linux":
            dl_engine_string = URL.create(
                drivername="redshift+psycopg2",
                database="stackadaptdev",
                host=redshift_dl_host_1,
                port=dl_port,
                username=rs_user,
                password=rs_password,
            )
            dl_engine = sa.create_engine(dl_engine_string, connect_args={"sslmode": "disable"})
        else:
            dl_engine_string = URL.create(
                drivername="redshift+psycopg2",
                database="stackadaptdev",
                host="127.0.0.1",
                port=16442,
            )
            # global dl_engine
            dl_engine = sa.create_engine(dl_engine_string, connect_args={"sslmode": "disable"})
            print("Daily Cluster: 1")
    elif mod_value == 2:
        if sysname == "Linux":
            dl_engine_string = URL.create(
                drivername="redshift+psycopg2",
                database="stackadaptdev",
                host=redshift_dl_host_2,
                port=dl_port,
                username=rs_user,
                password=rs_password,
            )
            dl_engine = sa.create_engine(dl_engine_string, connect_args={"sslmode": "disable"})
        else:
            dl_engine_string = URL.create(
                drivername="redshift+psycopg2",
                database="stackadaptdev",
                host="127.0.0.1",
                port=16444,
            )
            # global dl_engine
            dl_engine = sa.create_engine(dl_engine_string, connect_args={"sslmode": "disable"})
            print("Daily Cluster: 2")
    elif mod_value == 3:
        if sysname == "Linux":
            dl_engine_string = URL.create(
                drivername="redshift+psycopg2",
                database="stackadaptdev",
                host=redshift_dl_host_3,
                port=dl_port,
                username=rs_user,
                password=rs_password,
            )
            dl_engine = sa.create_engine(dl_engine_string, connect_args={"sslmode": "disable"})
        else:
            dl_engine_string = URL.create(
                drivername="redshift+psycopg2",
                database="stackadaptdev",
                host="127.0.0.1",
                port=16445,
            )
            # global dl_engine
            dl_engine = sa.create_engine(dl_engine_string, connect_args={"sslmode": "disable"})
            print("Daily Cluster: 3")


    return dl_engine


def mydb_conn_func():
    mydb_engine_string = URL.create(
        drivername="mysql+pymysql",
        database="nativead_production",
        host="127.0.0.1",
        port=10106,
    )
    # global mydb_engine
    mydb_engine = sa.create_engine(mydb_engine_string)

    return mydb_engine


def internal_analyticsdb_conn_func():
    internal_analyticsdb_engine_string = URL.create(
        drivername="mysql+pymysql",
        database="stackadaptanalytics",
        host="127.0.0.1",
        port=10502,
    )
    # global analyticsdb_engine
    analyticsdb_engine = sa.create_engine(internal_analyticsdb_engine_string)

    return analyticsdb_engine


def external_analyticsdb_conn_func():
    external_analyticsdb_conn_func = URL.create(
        drivername="mysql+pymysql",
        database="stackadaptanalytics",
        host="127.0.0.1",
        port=10500,
    )
    # global analyticsdb_engine
    analyticsdb_engine = sa.create_engine(external_analyticsdb_conn_func)

    return analyticsdb_engine


def statsdb_conn_func():
    statsdb_engne_string = URL.create(
        drivername="mysql+pymysql",
        database="nativead_production",
        host="127.0.0.1",
        port=10106,
    )
    # global statsdb_engine
    statsdb_engine = sa.create_engine(statsdb_engne_string)
    return statsdb_engine


def stats_tidb_conn_func():
    stats_tidb_engine_string = URL.create(
        drivername="mysql+pymysql", database="sa_stats", host="127.0.0.1", port=11428
    )
    stats_tidb_engine = sa.create_engine(stats_tidb_engine_string)
    return stats_tidb_engine


def app_info_db_conn_func():
    app_info_engine_string = URL.create(
        drivername="mysql+pymysql", database="app_info", host="127.0.0.1", port=10900
    )
    # global statsdb_engine
    app_info_engine = sa.create_engine(app_info_engine_string)
    return app_info_engine


# for pandas connection
def snowflake_pa_out_conn_func():
    script_path = f'/Users/{os.environ["USER"]}/Data/config.txt'
    exec(open(script_path).read())
    pa_out_engine_string = sURL(
        account=snowflake_creds["pa_sf_server"],
        user=snowflake_creds["pa_sf_uid"],
        password=snowflake_creds["pa_sf_pwd"],
        database=snowflake_creds["pa_sf_database"],
        schema=snowflake_creds["pa_sf_schema"],
        warehouse=snowflake_creds["pa_sf_wh"],
        role=snowflake_creds["pa_sf_role"],
    )

    snowflake_engine = sa.create_engine(pa_out_engine_string)
    return snowflake_engine


# for snowpark connection
def snowflake_session():
    if sysname == "Linux":

        def snowpark_session_create():
            private_key_path = "/home/ec2-user/snf_key.der"

            # Read the private key file content
            with open(private_key_path, "rb") as key_file:
                private_key = key_file.read()

            # Define connection parameters
            connection_params = {
                "account": "asbwzpt-stackadapt_pa",
                "user": pa_sf_uid,
                "role": "PA_REPORTING",
                "warehouse": pa_sf_warehouse,
                "private_key": private_key,
            }

            # Create and return the Snowpark session
            session = Session.builder.configs(connection_params).create()
            session.sql_simplifier_enabled = True
            return session

        session = snowpark_session_create()
    else:

        def snowpark_session_create():
            connection_params = {
                "account": snowflake_creds["pa_sf_server"],
                "user": snowflake_creds["pa_sf_uid"],
                "password": snowflake_creds["pa_sf_pwd"],
                "role": snowflake_creds["pa_sf_role"],
                "warehouse": snowflake_creds["pa_sf_wh"],
            }
            session = Session.builder.configs(connection_params).create()
            session.sql_simplifier_enabled = True
            return session

        session = snowpark_session_create()

    return session


common_metrics = [
    "sum(cost)/1000000.0 as cost",
    "sum(has_won) as has_won",
    "sum(has_click) as has_click",
    "sum(has_engagement) as has_engagement",
    "sum(total_time_on_site) as total_time_on_site",
    "sum(CASE WHEN total_time_on_site > 0 THEN 1 ELSE 0 END) as session_1s",
    "sum(has_conv) as has_conv",
    "sum(has_ltconv) as has_ltconv",
    "sum(has_secondary_conversion) as has_s_conv",
    "sum(has_s_ltconv) as has_s_ltconv",
    "sum(stats_moat_measure) as stats_moat_measure",
    "sum(stats_moat_inview) as stats_moat_inview",
    "sum(stats_vcomp_0) as stats_vcomp_0",
    "sum(stats_vcomp_25) as stats_vcomp_25",
    "sum(stats_vcomp_50) as stats_vcomp_50",
    "sum(stats_vcomp_75) as stats_vcomp_75",
    "sum(stats_vcomp_95) as stats_vcomp_95",
    "sum(stats_acomp_0) as stats_acomp_0",
    "sum(stats_acomp_25) as stats_acomp_25",
    "sum(stats_acomp_50) as stats_acomp_50",
    "sum(stats_acomp_75) as stats_acomp_75",
    "sum(stats_acomp_95) as stats_acomp_95",
    "sum(ltconv_revenue) as ltconv_revenue",
    "sum(campaign_conversion_revenue) as campaign_conversion_revenue",
    "sum(line_item_has_conversion_deduped) as line_item_has_conversion_deduped",
    "sum(advertiser_has_conversion_deduped) as advertiser_has_conversion_deduped",
    "sum(line_item_conversion_revenue_deduped) as line_item_conversion_revenue_deduped",
    "sum(advertiser_conversion_revenue_deduped) as advertiser_conversion_revenue_deduped",
    "sum(has_secondary_conversion) as has_secondary_conversion",
    "sum(advertiser_has_secondary_conversion_deduped) as advertiser_has_secondary_conversion_deduped",
    "sum(line_item_has_secondary_conversion_deduped) as line_item_has_secondary_conversion_deduped",
]


common_metrics1 = [
    "sum(cost)/1000000.0 as cost",
    "sum(has_won) as has_won",
    "sum(has_click) as has_click",
    "sum(has_engagement) as has_engagement",
    "sum(total_time_on_site) as total_time_on_site",
    "sum(CASE WHEN total_time_on_site > 0 THEN 1 ELSE 0 END) as session_1s",
    "sum(stats_moat_measure) as stats_moat_measure",
    "sum(stats_moat_inview) as stats_moat_inview",
    "sum(stats_vcomp_0) as stats_vcomp_0",
    "sum(stats_vcomp_25) as stats_vcomp_25",
    "sum(stats_vcomp_50) as stats_vcomp_50",
    "sum(stats_vcomp_75) as stats_vcomp_75",
    "sum(stats_vcomp_95) as stats_vcomp_95",
    "sum(stats_acomp_0) as stats_acomp_0",
    "sum(stats_acomp_25) as stats_acomp_25",
    "sum(stats_acomp_50) as stats_acomp_50",
    "sum(stats_acomp_75) as stats_acomp_75",
    "sum(stats_acomp_95) as stats_acomp_95",
]


# SnowFlake set up connection parameters
sf = {
    "account": "grb92414.us-east-1",
    "user": "<user_name>",
    "password": "<password>",
    "warehouse": "PATEAM",
    "database": "PA_OUT",
    "schema": "PUBLIC",
}

# pre-defining the dtypes of the mapping file field
mapping_data_types = {
    "user_id": "str",
    "user_name": "str",
    "timezone": "str",
    "start_date": "str",
    "end_date": "str",
    "sub_advertiser_id": "str",
    "sub_advertiser_name": "str",
    "line_item_id": "str",
    "line_item_name": "str",
    "line_item_cat1": "str",
    "line_item_cat2": "str",
    "campaign_id": "str",
    "campaign_name": "str",
    "campaign_supplytype": "str",
    "campaign_cat1": "str",
    "campaign_cat2": "str",
    "nativead_id": "str",
    "nativead_name": "str",
    "nativead_cat1": "str",
    "nativead_cat2": "str",
    "nativead_cat3": "str",
    "segment_id": "str",
    "segment_name": "str",
    "segment_type": "str",
    "segment_cat1": "str",
    "segment_cat2": "str",
    "conv_tracker_id": "str",
    "conv_tracker_name": "str",
    "conv_unique_key": "str",
    "conv_tracker_alias": "str",
}

# =============================================================================
# Please use this data_types to assign dtypes to the measures if you are using pd.groupby example, df = df.astype(cm_data_types)
# pre-defining dtypes of the measures in common_metrics
cm_data_types = {
    "cost": float,
    "has_won": int,
    "has_click": int,
    "has_engagement": int,
    "total_time_on_site": int,
    "session_1s": int,
    "has_conv": int,
    "has_ltconv": int,
    "stats_moat_measure": int,
    "stats_moat_inview": int,
    "stats_vcomp_0": int,
    "stats_vcomp_25": int,
    "stats_vcomp_50": int,
    "stats_vcomp_75": int,
    "stats_vcomp_95": int,
    "stats_acomp_0": int,
    "stats_acomp_25": int,
    "stats_acomp_50": int,
    "stats_acomp_75": int,
    "stats_acomp_95": int,
    "ltconv_revenue": int,
    "campaign_conversion_revenue": float,
    "line_item_has_conversion_deduped": float,
    "advertiser_has_conversion_deduped": float,
    "line_item_conversion_revenue_deduped": float,
    "advertiser_conversion_revenue_deduped": float,
    "has_secondary_conversion": int,
    "advertiser_has_secondary_conversion_deduped": int,
    "line_item_has_secondary_conversion_deduped": int,
}

# pre-defining dtypes of the measures in common_metrics1
cm1_data_types = {
    "cost": float,
    "has_won": int,
    "has_click": int,
    "has_engagement": int,
    "total_time_on_site": int,
    "session_1s": int,
    "stats_moat_measure": int,
    "stats_moat_inview": int,
    "stats_vcomp_0": int,
    "stats_vcomp_25": int,
    "stats_vcomp_50": int,
    "stats_vcomp_75": int,
    "stats_vcomp_95": int,
    "stats_acomp_0": int,
    "stats_acomp_25": int,
    "stats_acomp_50": int,
    "stats_acomp_75": int,
    "stats_acomp_95": int,
}


# =============================================================================


# Converting campaign start and end dates from account's local time to UTC time ----------------
def utc_translation(start_date, end_date, user_timezone):
    internal_analyticsdb_conn = internal_analyticsdb_conn_func()

    user_timezone_df = pd.read_sql(
        f"SELECT RS_timezone FROM 11_utc_adjust WHERE timezone = '{user_timezone}'",
        con=internal_analyticsdb_conn,
    )
    if user_timezone_df.shape[0] == 1:
        user_timezone = user_timezone_df["RS_timezone"][0]

    start_date_local = pd.Timestamp(ts_input=start_date, tz=user_timezone)
    start_date_utc = start_date_local.tz_convert("UTC").strftime("%Y-%m-%d %X")

    end_date_local = pd.Timestamp(ts_input=end_date, tz=user_timezone)
    end_date_utc = end_date_local.tz_convert("UTC").strftime("%Y-%m-%d %X")

    internal_analyticsdb_conn.dispose()

    return [start_date_utc, end_date_utc]


# Function Determining if user needs to use only the advertiser database or combo of both advertiser and daily---------------


def use_AdvTable_check(startdate, enddate, user_id):
    # =============================================================================
    #     global use_AdvTable
    #     global adv_max_time
    # =============================================================================

    RS_Adv_conn = RS_Adv_conn_func()
    RS_Daily_conn = RS_Daily_conn_func(user_id)

    query_daily_min_time = "SELECT MIN(table_name) FROM information_schema.tables WHERE table_name LIKE 'sa_rs_evt_table%%' and table_name not like '%%_back'"
    query_adv_max_time = " SELECT last_date as adv_max_time_utc FROM public.migration_status WHERE id = 1 "

    # end_date check
    if pd.Timestamp(startdate) >= pd.to_datetime("today"):
        global end_date
        end_date = (
            pd.to_datetime("today") - pd.Timedelta(1, unit="D")
        ).date().strftime("%Y-%m-%d") + " 23:59:59"

    daily_min_time = pd.read_sql(query_daily_min_time, con=RS_Daily_conn)
    daily_min_time = daily_min_time["min"][0]
    daily_min_time = pd.Timestamp(
        daily_min_time.replace("sa_rs_evt_table_", "").replace("_", "-")
    )
    print(f"Daily data available from: {daily_min_time}")

    adv_max_time = pd.read_sql(
        query_adv_max_time, con=RS_Adv_conn, parse_dates="adv_max_time_utc"
    )
    adv_max_time = adv_max_time["adv_max_time_utc"][0]

    # new logic daily cluster
    if pd.Timestamp(startdate) >= daily_min_time:
        use_AdvTable = 0  # using only daily cluster
    elif (pd.Timestamp(enddate) + pd.Timedelta(days=1)) <= adv_max_time:
        use_AdvTable = 1  # pulling only from archieve table
    # #Only used for rare scenerios
    # elif (pd.Timestamp(startdate) < daily_min_time) & ((adv_max_time < daily_min_time)):
    #     use_AdvTable = 3 # combo adv table + adv daily tables + daily cluster
    else:
        use_AdvTable = 2  # pulling only from archive table

    #  ==============================OLD LOGIC========================================
    # if pd.Timestamp(enddate) + pd.Timedelta(1,"D") <= adv_max_time :
    #     use_AdvTable = 1 # using only adv DB
    # elif pd.Timestamp(startdate) >= adv_max_time:
    #     use_AdvTable = 0 # using only daily DB + adv daily DB
    # else:
    #     use_AdvTable = 2 # using combo adv DB + adv daily DB + daily DB

    # if (pd.Timestamp(adv_max_time + pd.Timedelta("0 days 23:59:59"),tz= "UTC") < pd.Timestamp(enddate,tz="UTC")):
    #     adv_max_time = (adv_max_time + pd.Timedelta("0 days 23:59:59")).strftime("%Y-%m-%d %X")
    # else:
    #     adv_max_time = enddate
    #  =============================================================================

    RS_Adv_conn.dispose()
    return [use_AdvTable, adv_max_time, daily_min_time]
    # return [use_AdvTable,adv_max_time]


# test = pd.read_sql("SELECT * FROM exchange_rates limit 100 ",statsdb_engine)


def daily_looper_fun(dur, enddate, SELECT, WHERE_daily, GROUPBY, ORDERBY, user_id):
    RS_Adv_conn = RS_Adv_conn_func()
    RS_Daily_conn = RS_Daily_conn_func(user_id)
    daily_min_time = globals().get("daily_min_time") or os.getenv("DNAUTILS_DAILY_MIN_TIME")
    # daily_min_time = (daily_min_time - pd.Timedelta(1 ,unit="D")

    # setting global variables
    # =============================================================================
    #     global daily_data
    #     global daily_data_adv
    #     global daily_data_daily
    # =============================================================================

    adv_tables_df = pd.DataFrame(
        sa.inspect(RS_Adv_conn_func()).get_table_names(), columns=["tables"]
    )
    daily_tables_df = pd.DataFrame(
        sa.inspect(RS_Daily_conn_func(user_id)).get_table_names(), columns=["tables"]
    )
    duration_list = list(range(0, dur + 1))
    date_tbl = pd.DataFrame(
        list(
            map(
                lambda x: "sa_rs_evt_table_"
                + (
                    pd.to_datetime(enddate)
                    + pd.Timedelta(1, "D")
                    - pd.Timedelta(x, "D")
                ).strftime("%Y_%m_%d"),
                duration_list,
            )
        ),
        columns=["tables"],
    )

    # seeing how many tyables are found in dailytable
    daily_tbl_test = pd.DataFrame.dropna(pd.merge(daily_tables_df, date_tbl))
    # adv_tbl_test = pd.DataFrame.dropna(pd.merge(adv_tables_df, date_tbl))

    # duration_adv_daily = len(adv_tbl_test) - 1
    duration_daily = len(daily_tbl_test) - 1

    # duration for tables in dailydb
    duration_adv_daily = dur - duration_daily

    daily_data_adv = pd.DataFrame()
    daily_data_daily = pd.DataFrame()

    # sys.exit("Debugging")

    # if statement to see if we need to pull any daily tables from daily DB, if yes, pull necessary ones

    if duration_daily > 0:
        print("Pulling daily tables from daily DB")
        for X in range(0, duration_daily + 1):
            temp_table = "sa_rs_evt_table_" + (
                pd.to_datetime(enddate)
                + pd.Timedelta(1, "D")
                - pd.Timedelta(X, "D")
            ).strftime("%Y_%m_%d")
            print(temp_table)
            Combined_query = " ".join(
                (SELECT, "FROM", temp_table, WHERE_daily, GROUPBY, ORDERBY)
            )
            with RS_Daily_conn.connect() as conn:
                # Clear any inherited failed transaction state before executing.
                conn.exec_driver_sql("ROLLBACK")
                temp = pd.read_sql(Combined_query, con=conn)
            daily_data_daily = pd.concat([daily_data_daily, temp], ignore_index=True)

    # Only for rare scenarios
    if duration_adv_daily > 0:
        if daily_min_time is None:
            raise ValueError(
                "daily_min_time is required when pulling archived daily tables. "
                "Set DNAUTILS_DAILY_MIN_TIME (YYYY-MM-DD) or define daily_min_time first."
            )
        print("Pulling daily tables from Advertiser DB")
        for X in range(0, duration_adv_daily + 1):
            temp_table = "sa_rs_evt_table_" + (
                pd.to_datetime(daily_min_time, format="%Y-%m-%d")
                - pd.Timedelta(duration_daily, "D")
                - pd.Timedelta(X, "D")
            ).strftime("%Y_%m_%d")
            print(temp_table)
            Combined_query = " ".join(
                (SELECT, "FROM", temp_table, WHERE_daily, GROUPBY, ORDERBY)
            )
            with RS_Adv_conn.connect() as conn:
                # Clear any inherited failed transaction state before executing.
                conn.exec_driver_sql("ROLLBACK")
                temp = pd.read_sql(Combined_query, con=conn)
            daily_data_adv = pd.concat([daily_data_adv, temp], ignore_index=True)

    daily_data = pd.concat([daily_data_adv, daily_data_daily])

    RS_Adv_conn.dispose()
    RS_Daily_conn.dispose()
    return daily_data


def sbr_folder_path():
    internal_analyticsdb_conn = internal_analyticsdb_conn_func()
    df = pd.read_sql(
        "SELECT * FROM python_sbr_folder_paths", con=internal_analyticsdb_conn
    )
    pd.set_option("display.max_colwidth", None)
    if platform.system() != "Windows":
        sys_path = os.getcwd().split("/")
        username_idx = [x.casefold() for x in sys_path].index("users") + 1
        username = sys_path[username_idx]
    else:
        sys_path = os.getcwd().split("\\")
        username_idx = [x.casefold() for x in sys_path].index("users") + 1
        username = sys_path[username_idx]
    path = df[df["laptop_username"] == username]["SBR_folder_path"].to_string(
        index=False
    )
    internal_analyticsdb_conn.dispose()
    return path


def alias_extract(
    list_to_extract,
):  # helper function to extract alias from inputs to aggregate later on in scripts
    output = []
    for i in list_to_extract:
        lst = i.split(" as ")

        if len(lst) > 1:
            output.append(lst[-1])
        else:
            # if not aliased, append the name directly
            output.append(i)
    return output


def censor_pii(input_select_stmnt, timezone="UTC", hour_trunc=False, need_latlong = False):
    """
    DOCSTRING:
    ----------
    censor_pii takes SELECT statement string as input, modifies the statement so that it will censor PII appropriately when used in a query, and returns the modified SELECT statement.

    Parameters:
    ----------
    - pii_sharing_countries [variable] = "'in','us','pk','ng','bd','de','gb','fr','it','ke','es','sa','pe','my','au','cl','nl','cz','pt','se','ae','ch','lb','sg','no','nz','kw','jm','bn','ca'"
    - input_select_stmnt: String - String fromat select statement.
    - For Example:
    input_select_stmnt = '''SELECT
        network_id,
        region,
        zipcode,
        ip_address,
        has_engagement_tracker,
        device_geo_lat,
        has_conversion_tracker,
        sub_advertiser_id,
        line_item_id,
        campaign_id,
        supply_inventory_type,
        date_trunc('day', convert_timezone('US/Pacific',request_time)) as dt,
        CASE WHEN convert_timezone('US/Pacific',request_time) >= '2023-04-14 23:59' THEN 'group2' ELSE 'group1' END as date_category'''

    Returns:
    - output_select_stmnt - String format select statement.
    - For Example:
    output_select_stmnt = '''
    SELECT network_id,
       region,
       CASE
         WHEN country = 'us' THEN LEFT(zipcode, 5)
         ELSE zipcode
       END                                                             AS
       zipcode,
       CASE
         WHEN country NOT IN ( pii_sharing_countries)
               OR country = ''
               OR network_id = 211
               OR device_dnt = 1 THEN NULL
         ELSE ip_address
       END                                                             AS
       ip_address,
       has_engagement_tracker,
       CASE
         WHEN ( country = 'us'
                AND region IN ( 'ct', 'va', '' ) )
               OR network_id = 211
               OR device_dnt = 1 THEN NULL
         WHEN country NOT IN ( pii_sharing_countries)
               OR country = ''
               OR network_id = 211 THEN NULL
         ELSE device_geo_lat
       END                                                             AS
       device_geo_lat,
       has_conversion_tracker,
       sub_advertiser_id,
       line_item_id,
       campaign_id,
       supply_inventory_type,
       Date_trunc('day', Convert_timezone('US/Pacific', request_time)) AS dt,
       CASE
         WHEN Convert_timezone('US/Pacific', request_time) >= '2023-04-14 23:59'
       THEN
         'group2'
         ELSE 'group1'
       END                                                             AS
       date_category
    '''
    """

    # Cleaning input_select_stmnt - Remove spaces between text and a comma
    input_select_stmnt = re.sub(" +(?=,)", "", input_select_stmnt)

    # Columns to exclude from the SELECT statement
    never_share_cols = [
        "device_id_sha",
        "device_id_md5",
        "device_ifa",
        "win_price",
        "bid_price",
        "bid_reduction",
        "margin",
        "margin_adjusted",
        "conv_order_id",
        "email_recipient_address",
    ]
    never_share_pattern = "|".join(never_share_cols)
    input_select_stmnt = re.sub(never_share_pattern, "", input_select_stmnt)
    globals()["SELECT_groups"] = input_select_stmnt

    us_state_excl_cols = ["device_geo_lat", "device_geo_long"]
    us_safe_pii_cols = [
        "ip_address",
        "device_id",
        "last_ip",
        "user_agent",
        "liveramp_id",
        "request_duid",
        "most_associated_duid",
        "most_associated_ip",
        "most_associated_duid_tier",
        "request_duid_hashed",
        "most_associated_duid_hashed",
        "most_associated_ip_hashed",
        "hashed_ip",
    ]

    pii_cols = us_state_excl_cols + us_safe_pii_cols

    # other_cols is for alias removal use only
    other_cols = ["zipcode", "network_id"]

    # Combine all the columns to match pii columns only to remove aliases
    columns = pii_cols + other_cols

    for _ in columns:
        input_select_stmnt = re.sub(
            r"\b{}\b\s+AS\s+\w+".format(re.escape(_)),
            _,
            input_select_stmnt,
            flags=re.IGNORECASE,
        )

    pii_sharing_countries = "'in','us','pk','ng','bd','de','gb','fr','it','ke','es','sa','pe','my','au','cl','nl','cz','pt','se','ae','ch','lb','sg','no','nz','kw','jm','bn','ca'"
    us_state_loc_drop = "'ct', 'va'"
    output_select_stmnt = input_select_stmnt

    # if zipcode in input_select_stmnt then add case when statement that removes anything over 5 digits if country is US
    zip_col_update = """CASE
        WHEN country = 'us' THEN LEFT(zipcode, 5)
        WHEN country = 'ca' THEN LEFT(zipcode, 3)
        ELSE zipcode
          END AS zipcode"""

    output_select_stmnt = output_select_stmnt.replace("zipcode", zip_col_update)
    # request_time when there is a conversion

    if hour_trunc:
        conv_timestamp_stmnt = (
            f" DATE_TRUNC('hour', convert_timezone('{timezone}', request_time)) "
        )

        output_select_stmnt = output_select_stmnt.replace(
            "request_time", conv_timestamp_stmnt
        )
    else:
        print(
            "Warning: If you have included request_time wihtout date_trunc + convert timezone please set hour_trunc = True !!!"
        )

    if need_latlong:
        print(
            "Warning: Pulling LatLong! Please make sure that you are not sharing any PII col with Lat Long!!!"
        )
    else:
        for pii_col in pii_cols:
            if re.search(pii_col, output_select_stmnt):
                print("Warning: Found device lat/long with PII columns, removing it!!!")
                # Remove device_geo_lat and device_geo_long from the statement
                output_select_stmnt = re.sub(
                    r"\s*device_geo_lat\s*,?", "", output_select_stmnt
                )
                output_select_stmnt = re.sub(
                    r"\s*device_geo_long\s*,?", "", output_select_stmnt
                )

    for pii_col in pii_cols:
        if pii_col in us_state_excl_cols:
            us_state_loc_drop_update = f"""
                CASE
                    WHEN (country = 'us' AND region IN ({us_state_loc_drop},'')) OR network_id = 211 OR device_dnt = 1 THEN NULL
                    WHEN country NOT IN ({pii_sharing_countries}) OR country = '' OR network_id = 211 THEN NULL
                    ELSE {pii_col}
                END AS {pii_col}
            """
            output_select_stmnt = output_select_stmnt.replace(
                pii_col, us_state_loc_drop_update
            )
        else:
            if pii_col == "ip_address":
                us_safe_col_update = f"""
                CASE
                    WHEN has_conv = 1
                        OR has_secondary_conversion = 1
                        OR line_item_has_secondary_conversion_deduped = 1
                        OR advertiser_has_secondary_conversion_deduped = 1
                        OR has_ltconv = 1
                        OR has_s_ltconv = 1
                    THEN ip_address_sha256
                    WHEN country NOT IN ({pii_sharing_countries}) OR country ='' OR network_id = 211 OR device_dnt = 1 THEN NULL
                    ELSE {pii_col} 
                  END AS {pii_col} 
                  """
                output_select_stmnt = output_select_stmnt.replace(
                    f"{pii_col},", f"{us_safe_col_update},"
                )
            else:
                us_safe_col_update = f"""
                    CASE
                        WHEN country NOT IN ({pii_sharing_countries}) OR country = '' OR network_id = 211 OR device_dnt = 1 THEN NULL
                        ELSE {pii_col}
                    END AS {pii_col}
                """
                output_select_stmnt = output_select_stmnt.replace(
                    f"{pii_col},", f"{us_safe_col_update},"
                )

    return output_select_stmnt


def mapping_file_check(mapping_file_path):
    import os
    import platform
    import pandas as pd
    import numpy as np
    from datetime import datetime
    import re

    # from src.Query_Builder import QueryBuilder

    # ------------------------------------------Load Mapping File -----------------------------
    print(
        "Checking whether all nativead_ids & segment ids exists for the campaign ids in mapping file"
    )

    # Loading Mapping File
    if mapping_file_path.split(".")[-1] == "txt":
        input_data = pd.read_csv(
            f"{mapping_file_path}",
            sep="\t",
            encoding="latin-1",
            dtype=mapping_data_types,
        ).fillna("")

    #### FOR .XLSX FILES
    if mapping_file_path.split(".")[-1] == "xlsx":
        input_data = pd.read_excel(
            f"{mapping_file_path}", dtype=mapping_data_types
        ).fillna("")

    #### FOR .CSV FILES
    if mapping_file_path.split(".")[-1] == "csv":
        input_data = pd.read_csv(
            f"{mapping_file_path}", dtype=mapping_data_types
        ).fillna("")

    # -----------------------------Mapping File Values-----------------------------------------
    username = input_data["user_name"][0]
    start_dt = input_data["start_date"][0]
    end_dt = input_data["end_date"][0]
    user_timezone = input_data["timezone"][0]
    date_utc = utc_translation(start_dt, end_dt, user_timezone)
    start_date_utc = date_utc[0]
    end_date_utc = date_utc[1]
    user_id = input_data["user_id"][0]

    start_date_utc_less_year = (
        pd.to_datetime(start_date_utc) - pd.DateOffset(years=1)
    ).strftime(
        "%Y-%m-%d %X"
    )  # Lookback a year before campaign starts for creatives created

        
        
    start_date_utc_less_quarter = (
        pd.to_datetime(start_date_utc) - pd.DateOffset(months=3)
    ).strftime(
        "%Y-%m-%d %X"
    )
        
    end_date_utc_plus_quarter = (
        pd.to_datetime(end_date_utc) + pd.DateOffset(months=3)
    ).strftime(
        "%Y-%m-%d %X"
    )
        
    mapping_file_creative_ids = set(input_data["nativead_id"])
    mapping_file_segment_identifer = set(filter(None, input_data["segment_id"]))
    campaign_id_string = ",".join(filter(None, input_data["campaign_id"]))

    mydb_conn = mydb_conn_func()

    # Checking Date Formats
    date_format = "%Y-%m-%d %H:%M:%S"

    try:
        start_date_res = bool(datetime.strptime(start_dt, date_format))
    except ValueError:
        start_date_res = False

    try:
        end_date_res = bool(datetime.strptime(end_dt, date_format))
    except ValueError:
        end_date_res = False

    if start_date_res == False:
        print("Mapping File Start Date Format Incorrect ")
        input_data["start_date"][0] = start_dt.split()[0] + " 00:00:00"

    if end_date_res == False:
        print("Mapping File End Date Format Incorrect ")
        input_data["end_date"][0] = end_dt.split()[0] + " 23:59:59"

    if (start_date_res or end_date_res) == False:
        print(
            "Outputting Fixed Date Format Mapping File to Desktop - USE THIS ONE GOING FORWARD "
        )
        if platform.system() != "Windows":
            sys_path = os.getcwd().split("/")
            username_idx = [x.casefold() for x in sys_path].index("users") + 1
            computer_username = sys_path[username_idx]
            output_path = f"/Users/{computer_username}/Desktop/"

        else:
            sys_path = os.getcwd().split("\\")
            username_idx = [x.casefold() for x in sys_path].index("users") + 1
            computer_username = sys_path[username_idx]
            output_path = f"C:/Users/{computer_username}/Desktop/"

        fixed_date_filename = (
            f"{output_path}FixedDate_"
            + re.split(r"[\/\\/.]", mapping_file_path)[-2]
            + ".xlsx"
        )
        # input_data.to_csv(fixed_date_file, sep='\t', index=False)

        with pd.ExcelWriter(fixed_date_filename) as writer:
            input_data.to_excel(writer, sheet_name="Data", index=False)

    # =============================================================================
    #     print("""Please check mapping file if start/end date formatting matches %Y-%m-%d %H:%M:%S before dropping into Google Drive - See below prints if it matches. Excel formats it to 00:00:00 , but sometimes when read by Python/copied it's not correct, so you may have to manually fix it.""")
    #     print(f"Start Date Format is correct : {start_date_res}")
    #     print(f"End Date Format  is correct : {end_date_res}")
    # =============================================================================

    # Checking Nativead Format
    # input_data = pd.read_excel("/Users/ryan/Downloads/Incorrect Ruggable_SBR_Mapping_Annual Report.xlsx",dtype=mapping_data_types).fillna('')
    # input_data = pd.read_csv("/Users/ryan/Desktop/FixedDate_Incorrect Ruggable_SBR_Mapping_Annual Report.txt",sep='\t',encoding='latin-1',dtype=mapping_data_types).fillna('')
    nativead_id_list = input_data["nativead_id"].replace("", np.nan)
    nativead_id_list = nativead_id_list.dropna()
    nativead_id_list = pd.DataFrame(nativead_id_list)
    nativead_id_list["numeric_tf"] = nativead_id_list["nativead_id"].str.isnumeric()

    nativead_id_list_f = nativead_id_list.loc[nativead_id_list["numeric_tf"] == False]

    if nativead_id_list_f.empty == True:
        print("All nativead_id are all numeric - NO ACTION REQUIRED")
    else:
        nativead_id_list_f_string = nativead_id_list_f["nativead_id"].str.cat(sep=",")
        print(
            f"ACTION : Please look for these errors in mapping file creatives {nativead_id_list_f_string}"
        )

    # Checking Archived Campaigns existing under subadvertiser id(s)
    sub_advertiser_id_list = (
        input_data["sub_advertiser_id"].replace("", np.nan).dropna()
    )
    sub_advertiser_id_list = sub_advertiser_id_list.str.cat(sep=",")

    archived_cam_q = f"""SELECT id,name,start_date,end_date,archived_at,status_cd FROM campaigns WHERE archived_at IS NOT NULL AND status_cd != 4194304 AND sub_advertiser_id IN ({sub_advertiser_id_list}) 
    AND start_date >= '{start_date_utc}' AND end_date <= '{end_date_utc}' """

    archived_cam_results = pd.read_sql(archived_cam_q, con=mydb_conn)
    archived_cam_results["id"] = archived_cam_results["id"].astype("str")
    archived_cam_results_set = set(archived_cam_results["id"].astype("str"))
    cam_set = set(input_data["campaign_id"])
    cam_set.discard("")

    archived_cam_results_diff = list(archived_cam_results_set.difference(cam_set))
    archived_cam_results_cleaned = archived_cam_results[
        archived_cam_results["id"].isin(archived_cam_results_diff)
    ]

    if archived_cam_results_cleaned.empty == True:
        print(
            "No Archived Campaigns during reporting range under the selected sub_advertiser_ids"
        )
    else:
        print(
            """There are some archived campaigns(non draft) for this reporting range , please check with AM if these are to be included. Exporting Archived Campaign List Excel to Desktop
Note: I have removed archived campaigns that already exist in the mapping file since it's possible campaigns can be archived AFTER mapping file created"""
        )

        if platform.system() != "Windows":
            sys_path = os.getcwd().split("/")
            username_idx = [x.casefold() for x in sys_path].index("users") + 1
            computer_username = sys_path[username_idx]
            output_path = f"/Users/{computer_username}/Desktop/"

        else:
            sys_path = os.getcwd().split("\\")
            username_idx = [x.casefold() for x in sys_path].index("users") + 1
            computer_username = sys_path[username_idx]
            output_path = f"C:/Users/{computer_username}/Desktop/"

        with pd.ExcelWriter(
            f"{output_path}ArchivedCampaigns_{username}.xlsx"
        ) as writer:
            archived_cam_results_cleaned.to_excel(
                writer, sheet_name="ArchivedCampaigns", index=False
            )
            archived_cam_results.to_excel(
                writer, sheet_name="ArchivedCampaigns_all", index=False
            )

    # =============================================================================
    #     #Pulling Redshift Data to Compare against Mapping Data
    #
    #     rs_df = QueryBuilder(mapping_file_location = mapping_file_path ,groupby = ["campaign_id","nativead_id","matched_cseg_and_rt"],metrics = ["sum(cost)/1000000.0 as cost"] ).run_query(write=False)
    #
    #     rs_df_uniq_nativead_id = rs_df['nativead_id'].astype(str).unique()
    #     rs_df_uniq_segment_identifers = rs_df['matched_cseg_and_rt'].unique()
    #
    #     rs_df_uniq_nativead_id = set(rs_df_uniq_nativead_id)
    #     rs_df_uniq_segment_identifers = set(filter(None,rs_df_uniq_segment_identifers))
    # =============================================================================

    # -------------------------------------- MYSQL Pull Nativead Production - MYSQL Method -----------------------------------------

    # Checking if any draft campaigns are selected , added based on Angela's feedback
    # Though not sure if this is necessary because if campaign in draft and selected it is not running
    # So Essentially no mapping is nesscessary for RS data and won't affect the comparison logic below

    mysql_campaign_status_query = f""" SELECT id, name , status_cd FROM campaigns WHERE id IN ({campaign_id_string})"""
    mysql_campaign_status_tbl = pd.read_sql(mysql_campaign_status_query, con=mydb_conn)
    mysql_campaign_status_tbl_draft = mysql_campaign_status_tbl.loc[
        mysql_campaign_status_tbl["status_cd"] == 4194304, :
    ]

    if len(mysql_campaign_status_tbl_draft) != 0:
        print("Draft Campaigns Selected by AM")
        print(mysql_campaign_status_tbl_draft)
    else:
        print("No Draft Campaigns Selected")

    # Creatives
    # Query Native ads that are a year previous of campaign start date.
    mysql_nativead_query = f"""SELECT distinct native_ads.id, native_ads.name  FROM native_ads  
    INNER JOIN campaigns_native_ads ON native_ads.id = campaigns_native_ads.native_ad_id
    INNER JOIN ads_audit_trackers ON native_ads.id = ads_audit_trackers.native_ad_id
    WHERE campaigns_native_ads.campaign_id IN ({campaign_id_string}) 
    AND ads_audit_trackers.audit_status = 'passed'
    AND native_ads.created_at >= '{start_date_utc_less_year}' 
    AND native_ads.created_at <='{end_date_utc}'
    AND campaigns_native_ads.deleted_time IS NULL"""
    mysql_nativead_query_tbl = pd.read_sql(mysql_nativead_query, con=mydb_conn)
    
    
    
    #Creatives Deleted in Between
    
    mysql_nativead_query2 = f"""SELECT distinct native_ads.id, native_ads.name  FROM native_ads  
    INNER JOIN campaigns_native_ads ON native_ads.id = campaigns_native_ads.native_ad_id
    INNER JOIN ads_audit_trackers ON native_ads.id = ads_audit_trackers.native_ad_id
    WHERE campaigns_native_ads.campaign_id IN ({campaign_id_string}) 
    AND ads_audit_trackers.audit_status = 'passed'
    AND native_ads.created_at >= '{start_date_utc_less_year}' 
    AND native_ads.created_at <='{end_date_utc}'
    AND campaigns_native_ads.deleted_time >= '{start_date_utc_less_quarter}'
    AND campaigns_native_ads.deleted_time <= '{end_date_utc_plus_quarter}'"""
    mysql_nativead_query_tbl2 = pd.read_sql(mysql_nativead_query2, con=mydb_conn)
    
    mysql_nativead_query_tbl = pd.concat([mysql_nativead_query_tbl,mysql_nativead_query_tbl2],ignore_index = True)
    mysql_nativead_query_tbl.drop_duplicates(inplace=True)
    
    uniq_nativead_ids = set(mysql_nativead_query_tbl["id"].astype("str"))
    
    

    # Custom Segments

    mysql_cs_query = f"""SELECT distinct custom_segments.identifier,
    CASE WHEN custom_segments.display_name IS NULL THEN name ELSE custom_segments.display_name END as name,
    CASE
    WHEN (custom_segments.name LIKE '%%CRM >%%' OR  custom_segments.name LIKE '%%.csv%%') THEN 'CRM'
    WHEN (custom_segments.mode = 8 OR custom_segments.misc_info LIKE '%%"third_party_segments":["%%') AND (custom_segments.misc_info IS NULL OR NOT custom_segments.misc_info LIKE '%%"third_party_segments":[""]%%')  THEN 'Third Party'
    WHEN custom_segments.name LIKE '*StackAdapt >%%' OR campaigns_custom_segments.segment_type = 'data'  THEN 'Interest Segment'
    WHEN campaigns_custom_segments.segment_type = 'custom' THEN 'Custom Segment'
    WHEN custom_segments.mode = 9 THEN 'Lookalike Segment'
    WHEN custom_segments.mode = 12 THEN 'Audience Expansion'
    ELSE segment_type end as segment_type
    FROM custom_segments 
    LEFT JOIN campaigns_custom_segments ON custom_segments.id = campaigns_custom_segments.custom_segment_id 
    WHERE campaigns_custom_segments.campaign_id IN ({campaign_id_string}) 
    AND campaigns_custom_segments.deleted_at IS NULL
    AND (campaigns_custom_segments.bucket NOT IN ('age','gender','household income') OR campaigns_custom_segments.bucket IS NULL)
    AND custom_segments.identifier NOT LIKE '%%c_33N%%' 
    AND custom_segments.identifier NOT LIKE '%%c_1Fp%%'
    AND custom_segments.identifier NOT LIKE '%%c_33M%%'
    AND custom_segments.identifier NOT LIKE '%%c_1K6%%'
    AND custom_segments.identifier NOT LIKE '%%c_33P%%'
    AND custom_segments.identifier NOT LIKE '%%c_1EQ%%'
    AND custom_segments.identifier NOT LIKE '%%c_33Q%%'
    AND custom_segments.identifier NOT LIKE '%%c_1JJ%%'
    AND custom_segments.identifier NOT LIKE '%%c_33R%%'
    AND custom_segments.identifier NOT LIKE '%%c_1JK%%'
    AND custom_segments.identifier NOT LIKE '%%c_33S%%'
    AND custom_segments.identifier NOT LIKE '%%c_33T%%'
    AND custom_segments.identifier NOT LIKE '%%c_1F4%%'
    AND custom_segments.identifier NOT LIKE '%%c_33U%%'
    AND custom_segments.identifier NOT LIKE '%%c_3K1%%'
    AND custom_segments.identifier NOT LIKE '%%c_3kL%%'
    AND custom_segments.identifier NOT LIKE '%%c_34i%%'
    AND custom_segments.identifier NOT LIKE '%%c_1FJ%%'
    AND custom_segments.identifier NOT LIKE '%%c_34h%%'
    AND custom_segments.identifier NOT LIKE '%%c_34d%%'
    AND custom_segments.identifier NOT LIKE '%%c_1K7%%'
    AND custom_segments.identifier NOT LIKE '%%c_34g%%'
    AND custom_segments.identifier NOT LIKE '%%c_1K0%%'
    AND custom_segments.identifier NOT LIKE '%%c_34f%%'
    AND custom_segments.identifier NOT LIKE '%%c_1K9%%'
    AND custom_segments.identifier NOT LIKE '%%c_34e%%'
    AND custom_segments.identifier NOT LIKE '%%c_1K8%%'
    AND custom_segments.identifier NOT LIKE '%%c_34j%%'
    AND segment_type NOT IN ('shadow', 'negative','conversion_lookalike') """

    mysql_cs_query_tbl = pd.read_sql(mysql_cs_query, con=mydb_conn)
    
    
    
    
    
    #Custom Segments in Between :
    
    mysql_cs_query2 = f"""SELECT distinct custom_segments.identifier,
    CASE WHEN custom_segments.display_name IS NULL THEN name ELSE custom_segments.display_name END as name,
    CASE
    WHEN (custom_segments.name LIKE '%%CRM >%%' OR  custom_segments.name LIKE '%%.csv%%') THEN 'CRM'
    WHEN (custom_segments.mode = 8 OR custom_segments.misc_info LIKE '%%"third_party_segments":["%%') AND (custom_segments.misc_info IS NULL OR NOT custom_segments.misc_info LIKE '%%"third_party_segments":[""]%%')  THEN 'Third Party'
    WHEN custom_segments.name LIKE '*StackAdapt >%%' OR campaigns_custom_segments.segment_type = 'data'  THEN 'Interest Segment'
    WHEN campaigns_custom_segments.segment_type = 'custom' THEN 'Custom Segment'
    WHEN custom_segments.mode = 9 THEN 'Lookalike Segment'
    WHEN custom_segments.mode = 12 THEN 'Audience Expansion'
    ELSE segment_type end as segment_type
    FROM custom_segments 
    LEFT JOIN campaigns_custom_segments ON custom_segments.id = campaigns_custom_segments.custom_segment_id 
    WHERE campaigns_custom_segments.campaign_id IN ({campaign_id_string}) 
    AND campaigns_custom_segments.deleted_at >= '{start_date_utc_less_quarter}'
    AND campaigns_custom_segments.deleted_at <= '{end_date_utc_plus_quarter}'
    AND (campaigns_custom_segments.bucket NOT IN ('age','gender','household income') OR campaigns_custom_segments.bucket IS NULL)
    AND custom_segments.identifier NOT LIKE '%%c_33N%%' 
    AND custom_segments.identifier NOT LIKE '%%c_1Fp%%'
    AND custom_segments.identifier NOT LIKE '%%c_33M%%'
    AND custom_segments.identifier NOT LIKE '%%c_1K6%%'
    AND custom_segments.identifier NOT LIKE '%%c_33P%%'
    AND custom_segments.identifier NOT LIKE '%%c_1EQ%%'
    AND custom_segments.identifier NOT LIKE '%%c_33Q%%'
    AND custom_segments.identifier NOT LIKE '%%c_1JJ%%'
    AND custom_segments.identifier NOT LIKE '%%c_33R%%'
    AND custom_segments.identifier NOT LIKE '%%c_1JK%%'
    AND custom_segments.identifier NOT LIKE '%%c_33S%%'
    AND custom_segments.identifier NOT LIKE '%%c_33T%%'
    AND custom_segments.identifier NOT LIKE '%%c_1F4%%'
    AND custom_segments.identifier NOT LIKE '%%c_33U%%'
    AND custom_segments.identifier NOT LIKE '%%c_3K1%%'
    AND custom_segments.identifier NOT LIKE '%%c_3kL%%'
    AND custom_segments.identifier NOT LIKE '%%c_34i%%'
    AND custom_segments.identifier NOT LIKE '%%c_1FJ%%'
    AND custom_segments.identifier NOT LIKE '%%c_34h%%'
    AND custom_segments.identifier NOT LIKE '%%c_34d%%'
    AND custom_segments.identifier NOT LIKE '%%c_1K7%%'
    AND custom_segments.identifier NOT LIKE '%%c_34g%%'
    AND custom_segments.identifier NOT LIKE '%%c_1K0%%'
    AND custom_segments.identifier NOT LIKE '%%c_34f%%'
    AND custom_segments.identifier NOT LIKE '%%c_1K9%%'
    AND custom_segments.identifier NOT LIKE '%%c_34e%%'
    AND custom_segments.identifier NOT LIKE '%%c_1K8%%'
    AND custom_segments.identifier NOT LIKE '%%c_34j%%'
    AND segment_type NOT IN ('shadow', 'negative','conversion_lookalike') """
    
    mysql_cs_query_tbl2 = pd.read_sql(mysql_cs_query2, con=mydb_conn)

    mysql_cs_query_tbl = pd.concat([mysql_cs_query_tbl,mysql_cs_query_tbl2],ignore_index = True)
    mysql_cs_query_tbl.drop_duplicates(inplace=True)

    uniq_cs_ids = set(mysql_cs_query_tbl["identifier"].astype("str"))




    # RT Segments

    mysql_rt_query = f""" SELECT distinct rt_segments.id, 
    rt_segments.name ,
    'Retargeting' as segment_type
    FROM rt_segments 
    LEFT JOIN campaigns_rt_segments ON rt_segments.id = campaigns_rt_segments.rt_segment_id 
    WHERE campaigns_rt_segments.campaign_id IN ({campaign_id_string}) 
    AND campaigns_rt_segments.targeting_type = 1 
    AND campaigns_rt_segments.campaign_id IS NOT NULL 
    AND campaigns_rt_segments.deleted_at IS NULL ; """

    mysql_rt_query_tbl = pd.read_sql(mysql_rt_query, con=mydb_conn)
    
    




    mysql_rt_query2 = f""" SELECT distinct rt_segments.id, 
    rt_segments.name ,
    'Retargeting' as segment_type
    FROM rt_segments 
    LEFT JOIN campaigns_rt_segments ON rt_segments.id = campaigns_rt_segments.rt_segment_id 
    WHERE campaigns_rt_segments.campaign_id IN ({campaign_id_string}) 
    AND campaigns_rt_segments.targeting_type = 1 
    AND campaigns_rt_segments.campaign_id IS NOT NULL 
    AND campaigns_rt_segments.deleted_at >= '{start_date_utc_less_quarter}'
    AND campaigns_rt_segments.deleted_at <= '{end_date_utc_plus_quarter}'; """

    mysql_rt_query_tbl2 = pd.read_sql(mysql_rt_query2, con=mydb_conn)
    
    mysql_rt_query_tbl = pd.concat([mysql_rt_query_tbl,mysql_rt_query_tbl2],ignore_index = True)
    mysql_rt_query_tbl.drop_duplicates(inplace=True)
    
    
    uniq_rt_ids = set(mysql_rt_query_tbl["id"].astype("str"))

    mydb_conn.dispose()

    # -----------------------------------------Combine RT and CS Sets-----------------------------------------

    uniq_rt_cs_ids = uniq_cs_ids | uniq_rt_ids  # UNION

    # -----------------------------------------Set Comparison-----------------------------------------

    # Creatives Comparison

    if uniq_nativead_ids.issubset(mapping_file_creative_ids):
        print("No missing nativead ids in mapping file")
    else:
        print("Missing the below nativead ids in mapping file")
        print(uniq_nativead_ids.difference(mapping_file_creative_ids))

        # --------------------  Create DF of the missing nativead with ID and Name-------------------
        missing_native_ads = list(
            uniq_nativead_ids.difference(mapping_file_creative_ids)
        )

        missing_native_ads_tbl = mysql_nativead_query_tbl
        missing_native_ads_tbl["id"] = missing_native_ads_tbl["id"].astype(str)
        missing_native_ads_tbl = mysql_nativead_query_tbl[
            mysql_nativead_query_tbl["id"].isin(missing_native_ads)
        ]

    # Segments Comparison

    if uniq_rt_cs_ids.issubset(mapping_file_segment_identifer):
        print("No missing CS & RT identifers")
    else:
        print("Missing the below segment identifers in mapping file")
        print(uniq_rt_cs_ids.difference(mapping_file_segment_identifer))

        ##--------------------Create DF of missing CS & RT Segments#--------------------

        # CS

        missing_cs_rt = list(uniq_rt_cs_ids.difference(mapping_file_segment_identifer))
        missing_cs_tbl = mysql_cs_query_tbl
        missing_cs_tbl["identifier"] = missing_cs_tbl["identifier"].astype(str)
        missing_cs_tbl = missing_cs_tbl[
            missing_cs_tbl["identifier"].isin(missing_cs_rt)
        ]
        missing_cs_tbl = missing_cs_tbl.rename(columns={"identifier": "id"})

        # RT

        missing_rt_tbl = mysql_rt_query_tbl
        missing_rt_tbl["id"] = missing_rt_tbl["id"].astype(str)
        missing_rt_tbl = missing_rt_tbl[missing_rt_tbl["id"].isin(missing_cs_rt)]

        missing_cs_rt_tbl = pd.concat(
            [missing_cs_tbl, missing_rt_tbl], ignore_index=True
        )

    # Write an Excel File to Desktop if there is missing items

    if (
        (uniq_nativead_ids.issubset(mapping_file_creative_ids))
        and uniq_rt_cs_ids.issubset(mapping_file_segment_identifer)
    ) == False:
        print("Write Missing Items to Excel on Desktop")

        if platform.system() != "Windows":
            sys_path = os.getcwd().split("/")
            username_idx = [x.casefold() for x in sys_path].index("users") + 1
            computer_username = sys_path[username_idx]
            output_path = f"/Users/{computer_username}/Desktop/"

        else:
            sys_path = os.getcwd().split("\\")
            username_idx = [x.casefold() for x in sys_path].index("users") + 1
            computer_username = sys_path[username_idx]
            output_path = f"C:/Users/{computer_username}/Desktop/"

        if "missing_native_ads_tbl" in locals() and "missing_cs_rt_tbl" in locals():
            with pd.ExcelWriter(
                f"{output_path}Missing_Creatives&Segments_{username}.xlsx"
            ) as writer:
                missing_native_ads_tbl.to_excel(
                    writer, sheet_name="Missing_Creatives", index=False
                )
                missing_cs_rt_tbl.to_excel(
                    writer, sheet_name="Missing_CS_RT", index=False
                )
                print("Both Creatives & Segments Missing Export")
        elif "missing_native_ads_tbl" in locals():
            with pd.ExcelWriter(
                f"{output_path}Missing_Creatives&Segments_{username}.xlsx"
            ) as writer:
                missing_native_ads_tbl.to_excel(
                    writer, sheet_name="Missing_Creatives", index=False
                )
                print("Only Creatives Missing Export")
        else:
            with pd.ExcelWriter(
                f"{output_path}Missing_Creatives&Segments_{username}.xlsx"
            ) as writer:
                missing_cs_rt_tbl.to_excel(
                    writer, sheet_name="Missing_CS_RT", index=False
                )
                print("Only Segments Missing Export")

    # =============================================================================
    #     #Checking if RS is equal to Redshift IDs - Redshift Method
    #
    #     #Creatives
    #
    #     print("Comparing RS Pull vs Mapping File")
    #
    #     if mapping_file_creative_ids == rs_df_uniq_nativead_id:
    #         print("ALL Creative IDs are avaliable in mapping Ffle")
    #
    #     else :
    #         print("Missing the below native ad IDs in mapping file")
    #         print(rs_df_uniq_nativead_id.difference(mapping_file_creative_ids))
    #
    #     #Segment Identifiers
    #
    #     if mapping_file_segment_identifer == rs_df_uniq_segment_identifers:
    #         print("ALL Segment Identifers are avaliable in mapping file")
    #
    #     else :
    #         print("Missing the below segment identifers in mapping file")
    #         print(rs_df_uniq_segment_identifers.difference(mapping_file_segment_identifer))
    # =============================================================================

    return None


def archived_campaign_mapping_file(archived_cids, mapping_file_path):
    import os
    import platform
    import pandas as pd
    import numpy as np
    from datetime import datetime
    import re

    # Read Mapping File ---------------------

    # Loading Mapping File
    if mapping_file_path.split(".")[-1] == "txt":
        input_data = pd.read_csv(
            f"{mapping_file_path}",
            sep="\t",
            encoding="latin-1",
            dtype=mapping_data_types,
        ).fillna("")

    #### FOR .XLSX FILES
    if mapping_file_path.split(".")[-1] == "xlsx":
        input_data = pd.read_excel(
            f"{mapping_file_path}", dtype=mapping_data_types
        ).fillna("")

    #### FOR .CSV FILES
    if mapping_file_path.split(".")[-1] == "csv":
        input_data = pd.read_csv(
            f"{mapping_file_path}", dtype=mapping_data_types
        ).fillna("")

    # -----------------------------Mapping File Values-----------------------------------------
    username = input_data["user_name"][0]
    start_dt = input_data["start_date"][0]
    end_dt = input_data["end_date"][0]
    user_timezone = input_data["timezone"][0]
    date_utc = utc_translation(start_dt, end_dt, user_timezone)
    start_date_utc = date_utc[0]
    end_date_utc = date_utc[1]
    user_id = input_data["user_id"][0]

    start_date_utc_less_year = (
        pd.to_datetime(start_date_utc) - pd.DateOffset(years=1)
    ).strftime(
        "%Y-%m-%d %X"
    )  # Lookback a year before campaign starts for creatives created

    # Connection --------------------------------

    mydb_conn = mydb_conn_func()

    # Pulling user/sub_adv/line_itme/cam based on archived cids

    cam_query = f""" 
    Select
    campaigns.user_id,
    sub_advertisers.id as sub_advertiser_id,
    sub_advertisers.name as sub_advertiser_name,
    line_item_id,
    line_items.name as line_item_name,
    campaigns.id as campaign_id,
    campaigns.name as campaign_name,
    CASE WHEN campaigns.type = 'NativeCampaign' THEN 'Native'
    WHEN campaigns.type = 'DisplayCampaign' THEN 'Display'
    WHEN campaigns.type = 'VideoCampaign' THEN 'Video'
    WHEN campaigns.type = 'CTVCampaign' THEN 'CTV'
    WHEN campaigns.type = 'AudioCampaign' THEN 'Audio'
    ELSE campaigns.type END as campaign_supplytype
    
    FROM campaigns
    LEFT JOIN line_items ON campaigns.line_item_id = line_items.id
    LEFT JOIN sub_advertisers ON sub_advertisers.id = campaigns.sub_advertiser_id
    LEFT JOIN users ON users.id = campaigns.user_id
    
    WHERE campaigns.id IN ({archived_cids})    
    
    """

    cam_tbl = pd.read_sql(cam_query, con=mydb_conn)

    # Unique Values of sub_adv/line/campaign
    sub_adv_tbl = cam_tbl.loc[:, ["sub_advertiser_id", "sub_advertiser_name"]]
    sub_adv_tbl = sub_adv_tbl.drop_duplicates()

    line_item_tbl = cam_tbl.loc[:, ["line_item_id", "line_item_name"]]
    line_item_tbl = line_item_tbl.drop_duplicates()

    cam_tbl2 = cam_tbl.loc[:, ["campaign_id", "campaign_name", "campaign_supplytype"]]
    cam_tbl2 = cam_tbl2.drop_duplicates()

    # Creatives ---------------------------------

    mysql_nativead_query = f"""SELECT distinct native_ads.id, native_ads.name FROM native_ads  
    LEFT JOIN campaigns_native_ads ON native_ads.id = campaigns_native_ads.native_ad_id
    INNER JOIN ads_audit_trackers ON native_ads.id = ads_audit_trackers.native_ad_id
    WHERE campaigns_native_ads.campaign_id IN ({archived_cids}) 
    AND ads_audit_trackers.audit_status = 'passed'
    AND native_ads.created_at >= '{start_date_utc_less_year}' 
    AND native_ads.created_at <='{end_date_utc}' """
    mysql_nativead_query_tbl = pd.read_sql(mysql_nativead_query, con=mydb_conn)

    # Segments ----------------------------------

    # CS Segs

    mysql_cs_query = f"""SELECT distinct custom_segments.identifier,
    CASE WHEN custom_segments.display_name IS NULL THEN name ELSE custom_segments.display_name END as name,
    CASE
    WHEN (custom_segments.name LIKE '%%CRM >%%' OR  custom_segments.name LIKE '%%.csv%%') THEN 'CRM'
    WHEN (custom_segments.mode = 8 OR custom_segments.misc_info LIKE '%%"third_party_segments":["%%') AND (custom_segments.misc_info IS NULL OR NOT custom_segments.misc_info LIKE '%%"third_party_segments":[""]%%')  THEN 'Third Party'
    WHEN custom_segments.name LIKE '*StackAdapt >%%' OR campaigns_custom_segments.segment_type = 'data'  THEN 'Interest Segment'
    WHEN campaigns_custom_segments.segment_type = 'custom' THEN 'Custom Segment'
    WHEN campaigns_custom_segments.segment_type = 'app_install_audience_lookalike' THEN 'App Install Audience Lookalike'
    WHEN campaigns_custom_segments.segment_type = 'auto_exclude_converters' THEN 'Auto Exclude Converters'
    WHEN campaigns_custom_segments.segment_type = 'conversion_lookalike' THEN 'Conversion Lookalike'
    WHEN custom_segments.mode = 9 THEN 'Lookalike Segment'
    WHEN custom_segments.mode = 12 THEN 'Audience Expansion'
    ELSE segment_type end as segment_type
    FROM custom_segments 
    LEFT JOIN campaigns_custom_segments ON custom_segments.id = campaigns_custom_segments.custom_segment_id 
    WHERE campaigns_custom_segments.campaign_id IN ({archived_cids}) 
    AND campaigns_custom_segments.deleted_at IS NULL
    AND (campaigns_custom_segments.bucket NOT IN ('age','gender','household income') OR campaigns_custom_segments.bucket IS NULL)
    AND segment_type NOT IN ('shadow', 'negative') """

    mysql_cs_query_tbl = pd.read_sql(mysql_cs_query, con=mydb_conn)

    mysql_rt_query = f""" SELECT distinct rt_segments.id as identifier, 
    rt_segments.name ,
    'Retargeting' as segment_type
    FROM rt_segments 
    LEFT JOIN campaigns_rt_segments ON rt_segments.id = campaigns_rt_segments.rt_segment_id 
    WHERE campaigns_rt_segments.campaign_id IN ({archived_cids}) 
    AND campaigns_rt_segments.targeting_type = 1 
    AND campaigns_rt_segments.campaign_id IS NOT NULL 
    AND campaigns_rt_segments.deleted_at IS NULL ; """

    mysql_rt_query_tbl = pd.read_sql(mysql_rt_query, con=mydb_conn)

    combined_rt_cs_tbl = pd.concat(
        [mysql_cs_query_tbl, mysql_rt_query_tbl], ignore_index=True
    )

    # Conversion Trackers -----------------------
    conv_tracker_q = f"""SELECT  distinct conversion_trackers.id,conversion_trackers.name,conversion_trackers.unique_key 
    FROM campaigns_conversion_trackers
    LEFT JOIN conversion_trackers ON campaigns_conversion_trackers.conversion_tracker_id = conversion_trackers.id  WHERE campaign_id IN ({archived_cids})"""

    conv_tracker_q_tbl = pd.read_sql(conv_tracker_q, con=mydb_conn)

    # Comparison ----------------------------

    # Converting to Set

    # Mapping File

    # Sub_adv
    mapping_sub_adv_list = input_data["sub_advertiser_id"].replace("", np.nan)
    mapping_sub_adv_list = mapping_sub_adv_list.dropna()
    mapping_sub_adv_set = set(mapping_sub_adv_list)

    # Creatives
    mapping_nativead_id_list = input_data["nativead_id"].replace("", np.nan)
    mapping_nativead_id_list = mapping_nativead_id_list.dropna()
    mapping_nativead_id_set = set(mapping_nativead_id_list)

    # Segments
    mapping_segs_list = input_data["segment_id"].replace("", np.nan)
    mapping_segs_list = mapping_segs_list.dropna()
    mapping_segs_set = set(mapping_segs_list)

    # Trackers

    mapping_convtracker_list = input_data["conv_tracker_id"].replace("", np.nan)
    mapping_convtracker_list = mapping_convtracker_list.dropna()
    mapping_convtracker_set = set(mapping_convtracker_list)

    # MYSQL query set

    # Sub adv
    mysql_sub_adv_set = set(sub_adv_tbl["sub_advertiser_id"].astype(str))

    # Creatives
    mysql_nativead_set = set(mysql_nativead_query_tbl["id"].astype(str))

    # Segments
    mysql_seg_set = set(combined_rt_cs_tbl["identifier"].astype(str))

    # Conv_tracker

    mysql_conv_tracker_set = set(conv_tracker_q_tbl["id"].astype(str))

    # Set Diff

    sub_adv_dff = list(map(int, mysql_sub_adv_set.difference(mapping_sub_adv_set)))
    nativead_diff = list(
        map(int, mysql_nativead_set.difference(mapping_nativead_id_set))
    )
    seg_diff = list(mysql_seg_set.difference(mapping_segs_set))
    conv_tracker_diff = list(
        map(int, mysql_conv_tracker_set.difference(mapping_convtracker_set))
    )

    sub_adv_output = sub_adv_tbl[sub_adv_tbl["sub_advertiser_id"].isin(sub_adv_dff)]
    nativead_output = mysql_nativead_query_tbl[
        mysql_nativead_query_tbl["id"].isin(nativead_diff)
    ]
    seg_output = combined_rt_cs_tbl[combined_rt_cs_tbl["identifier"].isin(seg_diff)]
    conv_tracker_output = conv_tracker_q_tbl[
        conv_tracker_q_tbl["id"].isin(conv_tracker_diff)
    ]

    if platform.system() != "Windows":
        sys_path = os.getcwd().split("/")
        username_idx = [x.casefold() for x in sys_path].index("users") + 1
        computer_username = sys_path[username_idx]
        output_path = f"/Users/{computer_username}/Desktop/"

    else:
        sys_path = os.getcwd().split("\\")
        username_idx = [x.casefold() for x in sys_path].index("users") + 1
        computer_username = sys_path[username_idx]
        output_path = f"C:/Users/{computer_username}/Desktop/"

    with pd.ExcelWriter(
        f"{output_path}ArchivedCamMappingData_{username}.xlsx"
    ) as writer:
        sub_adv_output.to_excel(writer, sheet_name="Sub_adv", index=False)
        cam_tbl2.to_excel(writer, sheet_name="Cams", index=False)
        nativead_output.to_excel(writer, sheet_name="Nativead", index=False)
        seg_output.to_excel(writer, sheet_name="Segs", index=False)
        conv_tracker_output.to_excel(writer, sheet_name="Conv_tracker", index=False)
        print("Archived Campaign Data Exported, only non duplicated from mapping file.")

    return None


sf_pa_passcode = 0


def ask_for_input():
    print(
        "Please go to DuoMobile on your phone and get the MFA code for Snowflake (PA Cluster)"
    )
    passcode = input("Enter your MFA Passcode: ")

    print(f"The passcode your entered is: {passcode} \n")
    return passcode


# Once MFA is onboarded for all, this will be replace regular snowflake_session()
def snowflake_session(MFA=True):
    if sysname == "Linux":

        def snowpark_session_create():
            connection_params = {
                "account": pa_sf_odbc,
                "user": pa_sf_uid,
                "password": sf_pwd,
                "role": "PA_REPORTING",
                "warehouse": pa_sf_warehouse,
            }
            session = Session.builder.configs(connection_params).create()
            session.sql_simplifier_enabled = True
            return session

        session = snowpark_session_create()
    else:
        if MFA == True:
            pa_passcode = ask_for_input()

            def snowpark_session_create():
                connection_params = {
                    "account": snowflake_creds["pa_sf_server"],
                    "user": snowflake_creds["pa_sf_uid"],
                    "password": snowflake_creds["pa_sf_pwd"],
                    "role": snowflake_creds["pa_sf_role"],
                    "warehouse": snowflake_creds["pa_sf_wh"],
                    "passcode": pa_passcode,
                }

                session = Session.builder.configs(connection_params).create()
                session.sql_simplifier_enabled = True
                return session

            session = snowpark_session_create()
        else:

            def snowpark_session_create():
                connection_params = {
                    "account": snowflake_creds["pa_sf_server"],
                    "user": snowflake_creds["pa_sf_uid"],
                    "password": snowflake_creds["pa_sf_pwd"],
                    "role": snowflake_creds["pa_sf_role"],
                    "warehouse": snowflake_creds["pa_sf_wh"],
                }

                session = Session.builder.configs(connection_params).create()
                session.sql_simplifier_enabled = True
                return session

            session = snowpark_session_create()
    print("Connected Sucessfully!")
    return session
