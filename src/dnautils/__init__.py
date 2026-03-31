from __future__ import annotations

from .legacy import call


__all__ = [
    "RS_Adv_conn_func",
    "RS_Daily_conn_func",
    "mydb_conn_func",
    "daily_looper_fun",
    "utc_translation",
    "censor_pii",
]


def RS_Adv_conn_func():
    return call("RS_Adv_conn_func")


def RS_Daily_conn_func(user_id):
    return call("RS_Daily_conn_func", user_id)


def mydb_conn_func():
    return call("mydb_conn_func")


def daily_looper_fun(dur, enddate, SELECT, WHERE_daily, GROUPBY, ORDERBY, user_id):
    return call("daily_looper_fun", dur, enddate, SELECT, WHERE_daily, GROUPBY, ORDERBY, user_id)


def utc_translation(start_date, end_date, user_timezone):
    return call("utc_translation", start_date, end_date, user_timezone)


def censor_pii(input_select_stmnt, timezone="UTC", hour_trunc=False, need_latlong=False):
    return call(
        "censor_pii",
        input_select_stmnt,
        timezone=timezone,
        hour_trunc=hour_trunc,
        need_latlong=need_latlong,
    )
