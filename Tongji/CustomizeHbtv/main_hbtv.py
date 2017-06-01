# -*- coding: utf-8 -*-
import __init__
from Store.StoreCustomized import StoreCustomized
from SaaSConfig.config import set_uvfile_path_format
from SaaSConfig.config import set_log_path_format_full
from SaaSConfig.config import set_log_path_format
from DBClient.MysqlData import MysqlData
import sys

def params(datatype):
    mysqldata = MysqlData()
    dbname, datatype, plat = mysqldata.getCustomParms(datatype)
    return dbname, datatype, plat

if __name__ == "__main__":
    set_log_path_format("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/????.log.gz")
    set_log_path_format_full("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log.gz")
    set_uvfile_path_format("/data1/logs/uvfilesaas/%(datatype)s/uvfile_%(yyyymmdd)s.log")

    datatypes = ["hbtv"]

    if "appoint" in sys.argv:
        # num, dbname, datatype, plat, appoint, ifdel=True, ifclearresult=True
        for i in [1,]:
            process = StoreCustomized()
            # process.storeappointbynum(i, "jh_10a0e81221095bdba91f7688941948a6", "BIQU_ANDROID", "android", "custom_hx_searchsub", logtype="logfile", ifdel=True)
            process.storeappointbydaily(i, "guaeng", "hbtv", "android", "overall_custom", ifdel=True, ifclearresult=True)
            # process.storeappointbydaily(i, "jh_10a0e81221095bdba91f7688941948a6", "biqu", "ios", "custom_hx_searchbefore", ifdel=True, ifclearresult=True)
            # process.storeappointbynum(i, "jh_10a0e81221095bdba91f7688941948a6", "biqu", "ios", "custom_hx_searchbetween", logtype="logfile", ifdel=True)

    if "store" in sys.argv:
        last_days = [i for i in range(2, 3)]
        last_days.reverse()
        for i in last_days:
            for datatype in datatypes:
                process = StoreCustomized()
                dbname, datatype, plat = params(datatype)
                process.storeByDaily(i, dbname, datatype, plat, ifdel=True)

    if "daily" in sys.argv:
        for datatype in datatypes:
            try:
                process = StoreCustomized()
                dbname, datatype, plat = params(datatype)
                process.storeByDaily(1, dbname, datatype, plat, ifdel=True)
            except:
                import traceback
                print(traceback.print_exc())
