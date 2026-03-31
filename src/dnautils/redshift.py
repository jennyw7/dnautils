from __future__ import annotations

import os
import platform

import pandas as pd
import sqlalchemy as sa
from sqlalchemy.engine.url import URL

from .legacy import call, load_legacy_module

_SYSNAME = platform.uname()[0]

# Keep default connection behavior consistent with the original script.
os.environ.setdefault("REDSHIFT_SSLMODE", "disable")


def _load_pykeys_values() -> dict:
    try:
        from pyKeys import (  # type: ignore
            dl_port,
            redshift_dl_host_0,
            redshift_dl_host_1,
            redshift_dl_host_2,
            redshift_dl_host_3,
            rs_password,
            rs_user,
        )
    except Exception as exc:  # pragma: no cover - env-specific import
        raise ImportError(
            "pyKeys import failed. Ensure pyKeys.py is on PYTHONPATH for Linux Redshift connections."
        ) from exc

    return {
        "dl_port": dl_port,
        "redshift_dl_host_0": redshift_dl_host_0,
        "redshift_dl_host_1": redshift_dl_host_1,
        "redshift_dl_host_2": redshift_dl_host_2,
        "redshift_dl_host_3": redshift_dl_host_3,
        "rs_user": rs_user,
        "rs_password": rs_password,
    }


def RS_Daily_conn_func(user_id):
    mod_value = user_id % 4
    print(f"Mod Value : {mod_value}")

    if _SYSNAME == "Linux":
        creds = _load_pykeys_values()
        host_by_mod = {
            0: creds["redshift_dl_host_0"],
            1: creds["redshift_dl_host_1"],
            2: creds["redshift_dl_host_2"],
            3: creds["redshift_dl_host_3"],
        }

        dl_engine_string = URL.create(
            drivername="redshift+psycopg2",
            database="stackadaptdev",
            host=host_by_mod[mod_value],
            port=creds["dl_port"],
            username=creds["rs_user"],
            password=creds["rs_password"],
        )
        return sa.create_engine(dl_engine_string, connect_args={"sslmode": "disable"})

    port_by_mod = {0: 16441, 1: 16442, 2: 16444, 3: 16445}
    dl_engine_string = URL.create(
        drivername="redshift+psycopg2",
        database="stackadaptdev",
        host="127.0.0.1",
        port=port_by_mod[mod_value],
    )
    print(f"Daily Cluster: {mod_value}")
    return sa.create_engine(dl_engine_string, connect_args={"sslmode": "disable"})


def _resolve_daily_min_time() -> pd.Timestamp:
    module = load_legacy_module()
    value = getattr(module, "daily_min_time", None)
    if value is None:
        value = os.getenv("DNAUTILS_DAILY_MIN_TIME")
    if value is None:
        raise ValueError(
            "daily_min_time is required when archive daily tables are needed. "
            "Set DNAUTILS_DAILY_MIN_TIME (YYYY-MM-DD) or initialize daily_min_time in legacy context."
        )
    return pd.to_datetime(value, format="%Y-%m-%d")


def daily_looper_fun(dur, enddate, SELECT, WHERE_daily, GROUPBY, ORDERBY, user_id):
    rs_adv_conn = call("RS_Adv_conn_func")
    rs_daily_conn = RS_Daily_conn_func(user_id)

    daily_tables_df = pd.DataFrame(
        sa.inspect(rs_daily_conn).get_table_names(), columns=["tables"]
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

    daily_tbl_test = pd.DataFrame.dropna(pd.merge(daily_tables_df, date_tbl))

    duration_daily = len(daily_tbl_test) - 1
    duration_adv_daily = dur - duration_daily

    daily_data_adv = pd.DataFrame()
    daily_data_daily = pd.DataFrame()

    if duration_daily > 0:
        print("Pulling daily tables from daily DB")
        for x in range(0, duration_daily + 1):
            temp_table = "sa_rs_evt_table_" + (
                pd.to_datetime(enddate)
                + pd.Timedelta(1, "D")
                - pd.Timedelta(x, "D")
            ).strftime("%Y_%m_%d")
            print(temp_table)
            combined_query = " ".join(
                (SELECT, "FROM", temp_table, WHERE_daily, GROUPBY, ORDERBY)
            )
            with rs_daily_conn.connect() as conn:
                conn.exec_driver_sql("ROLLBACK")
                temp = pd.read_sql(combined_query, con=conn)
            daily_data_daily = pd.concat([daily_data_daily, temp], ignore_index=True)

    if duration_adv_daily > 0:
        daily_min_time = _resolve_daily_min_time()
        print("Pulling daily tables from Advertiser DB")
        for x in range(0, duration_adv_daily + 1):
            temp_table = "sa_rs_evt_table_" + (
                daily_min_time - pd.Timedelta(duration_daily, "D") - pd.Timedelta(x, "D")
            ).strftime("%Y_%m_%d")
            print(temp_table)
            combined_query = " ".join(
                (SELECT, "FROM", temp_table, WHERE_daily, GROUPBY, ORDERBY)
            )
            with rs_adv_conn.connect() as conn:
                conn.exec_driver_sql("ROLLBACK")
                temp = pd.read_sql(combined_query, con=conn)
            daily_data_adv = pd.concat([daily_data_adv, temp], ignore_index=True)

    daily_data = pd.concat([daily_data_adv, daily_data_daily])

    rs_adv_conn.dispose()
    rs_daily_conn.dispose()
    return daily_data