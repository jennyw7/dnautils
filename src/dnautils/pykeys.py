from __future__ import annotations

import os
import platform
import re


sysname = platform.uname()[0]


def check_env(temp_name: str, temp_replacement):
    temp_out = os.getenv(temp_name)
    if temp_out is None:
        temp_out = temp_replacement
    if bool(re.search("PORT", temp_name)):
        temp_out = int(temp_out)
    return temp_out


# Universal Variables
universal_host = "127.0.0.1"
analytics_universaldb = "stackadaptanalytics"

host = os.getenv("PA_INTERNAL_HOST")
analytics_dbname = analytics_universaldb
websql_user = os.getenv("PA_WEBSQL_USER")
websql_password = os.getenv("PA_WEBSQL_PWD")

# Snowflake Credentials
snowflake_account_identifier = "stackadapt.us-east-1"
sf_uid = os.getenv("SNOWFLAKE_USER")
sf_pwd = os.getenv("SNOWFLAKE_PWD")
sf_odbc = "MCL_snowflake"
sf_db = "MCCLATCHY"
sf_schema = "PUBLIC"
sf_db_sc = "SOUTHCOLLEGE"

if sysname == "Linux":
    sf_odbc = "snowflake"

pa_sf_uid = "Andrey"
pa_sf_odbc = "asbwzpt-stackadapt_pa"
pa_sf_warehouse = "PATEAM"

dw_sf_odbc = "ASBWZPT-STACKADAPT_DW"
dw_sf_uid = check_env("DW_SNOWFLAKE_USER", "")
dw_sf_pwd = check_env("DW_SNOWFLAKE_PWD", "")
dw_sf_warehouse = "FINANCE_DATA_WAREHOUSE"

# RedShift Daily Credentials
redshift_dl_host = rs_host = dl_host = check_env("PA_REDSHIFT_DL_HOST", universal_host)
redshift_dl_port = rs_port = port = dl_port = check_env("PA_REDSHIFT_DL_PORT", 20100)
redshift_dl_user = rs_user = user = check_env("PA_REDSHIFT_DL_USER", "")
redshift_dl_pwd = rs_password = password = check_env("PA_REDSHIFT_DL_PWD", "")
redshift_dl_dbname = rs_dbname = dbname = "stackadaptdev"

redshift_dl_host_0 = check_env("PA_REDSHIFT_DL_HOST_0", universal_host)
redshift_dl_host_1 = check_env("PA_REDSHIFT_DL_HOST_1", universal_host)
redshift_dl_host_2 = check_env("PA_REDSHIFT_DL_HOST_2", universal_host)
redshift_dl_host_3 = check_env("PA_REDSHIFT_DL_HOST_3", universal_host)

redshift_dl_port_0 = check_env("PA_REDSHIFT_DL_PORT_0", 16441)
redshift_dl_port_1 = check_env("PA_REDSHIFT_DL_PORT_1", 16442)
redshift_dl_port_2 = check_env("PA_REDSHIFT_DL_PORT_2", 16444)
redshift_dl_port_3 = check_env("PA_REDSHIFT_DL_PORT_3", 16445)

# RedShift Advertiser Credentials
redshift_adv_host = arch_host = adv_host = check_env("PA_REDSHIFT_ADV_HOST", universal_host)
redshift_adv_port = adv_port = check_env("PA_REDSHIFT_DL_PORT", 20200)
redshift_adv_user = check_env("PA_REDSHIFT_DL_USER", "")
redshift_adv_pwd = check_env("PA_REDSHIFT_DL_PWD", "")
redshift_adv_dbname = "stackadaptdev"

# WebSQL Credential
websql_host = check_env("PA_WEBSQL_HOST", universal_host)
websql_port = stats_port = check_env("PA_WEBSQL_PORT", 10106)
websql_user = check_env("PA_WEBSQL_USER", "")
websql_password = check_env("PA_WEBSQL_PWD", "")
websql_dbname = "nativead_production"

# MySQL Stats Credentials
stats_host = st_host = statsdb_host = check_env("PA_STATS_HOST", universal_host)
stats_port = check_env("PA_STATS_PORT", 10106)
stats_user = st_user = statsdb_user = check_env("PA_STATS_USER", "")
stats_password = st_password = statsdb_password = check_env("PA_STATS_PWD", "")
stats_dbname = st_dbname = statsdb_name = "StackAdapt_Stats"

# Internal PA server
analytics_internal_host = new_analytics_host = check_env("PA_INTERNAL_HOST", universal_host)
analytics_internal_port = internal_port = check_env("PA_INTERNAL_PORT", 10502)
analytics_internal_user = check_env("PA_INTERNAL_USER", "")
analytics_internal_password = check_env("PA_INTERNAL_PWD", "")
analytics_internal_dbname = analytics_universaldb

# External PA server
analytics_external_host = new_analytics_ext = check_env("PA_EXTERNAL_HOST", universal_host)
analytics_external_port = external_port = check_env("PA_EXTERNAL_PORT", 10500)
analytics_external_user = websql_user_ext = check_env("PA_EXTERNAL_USER", "")
analytics_external_password = websql_password_ext = check_env("PA_EXTERNAL_PWD", "")
analytics_external_dbname = analytics_universaldb

# Old PA Server
analytics_cluster_host = new_analytics_old = check_env("PA_OLD_HOST", universal_host)
analytics_cluster_port = mysql_prod = check_env("PA_OLD_PORT", 10504)
analytics_cluster_user = websql_user_old = check_env("PA_OLD_USER", "")
analytics_cluster_password = websql_password_old = check_env("PA_OLD_PWD", "")
analytics_cluster_dbname = analytics_universaldb

# Partnership DB
pi_host = check_env("PA_PDB_HOST", universal_host)
pi_port = check_env("PA_PDB_PORT", 11110)
pi_user = check_env("PA_PDB_USER", "")
pi_password = check_env("PA_PDB_PWD", "")
pi_dbname = "data"
