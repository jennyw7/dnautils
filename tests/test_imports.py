import dnautils as cfl


def test_public_exports_exist():
    assert hasattr(cfl, "RS_Adv_conn_func")
    assert hasattr(cfl, "RS_Daily_conn_func")
    assert hasattr(cfl, "daily_looper_fun")
