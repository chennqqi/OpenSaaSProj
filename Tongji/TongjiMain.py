# -*- coding: utf-8 -*-
import __init__
from DBClient.MysqlClient import MysqlClient

from SaaSConfig.config import set_uvfile_path_format
from SaaSConfig.config import set_log_path_format_full
from SaaSConfig.config import set_log_path_format
from SaaSConfig.config import mongo_ids
from TongjiCommands import TongjiCommands
import time
import sys

mongo_id = mongo_ids

# Appkey 与 datatype 对应
def GetAppkey_saas():
    plats = ["ios", "android", "all", "feeling"]
    client = MysqlClient("saas_server")
    result = client.getAppkey_kwargs(plat = plats, mongo_id=mongo_id, filter_keys=["appkey", "cdkey"])
    data = {}
    for item in result:
        print item
        dbname, appkey, plat = item["cdkey"], item["appkey"], item["plat"]
        if (dbname, plat) not in data.get(appkey, []):
            data.setdefault(appkey, []).append((dbname, plat))
    client.closeMysql()
    return data

# Appkey 与 datatype 对应
def GetAppkey_h5():
    plats = ["h5"]
    client = MysqlClient("saas_server")
    # result = client.getAppkey_h5()
    result = client.getAppkey_kwargs(plat = plats, mongo_id=mongo_id, filter_keys=["appkey", "cdkey"])
    data = {}
    for item in result:
        dbname, appkey, plat = item["cdkey"], item["appkey"], item["plat"]
        if (dbname, plat) not in data.get(appkey, []):
            data.setdefault(appkey, []).append((dbname, plat))
    client.closeMysql()
    return data


def GetAppkey():
    data = {}
    data.setdefault("guaengdemo", ("guaengdemo", "ios"))
    return data


def GetAppkeys(**kwargs):
    client = MysqlClient("saas_server")
    data = client.getAppkey_kwargs(**kwargs)
    result = {}
    for item in data:
        appkey = item["appkey"]
        dbname = item["cdkey"]
        plat = item["plat"]
        if (dbname, plat) not in result.get(appkey, []):
            result.setdefault(appkey, []).append((dbname, plat))
    client.closeMysql()
    return result


if __name__ == "__main__":


    if "test" in sys.argv:
        set_log_path_format("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/????.log.gz")
        set_log_path_format_full("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log.gz")
        set_uvfile_path_format("/data1/logs/uvfilesaas/%(datatype)s/uvfile_%(yyyymmdd)s.log")
        # for i in range(2, 8):
        #     TongjiCommands.appointStore_bydaily(i, "ncf", "ncf_h5", "h5", "ref_source", "h5", True)
        data = GetAppkey_saas()
        for datatype in data:
            for item in data[datatype]:
                dbname, plat = item
                TongjiCommands.store_udf(1, dbname, datatype, plat, ["store_mapmeta"])

    if "test_all" in sys.argv:
        set_log_path_format("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/????.log.gz")
        set_log_path_format_full("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log.gz")
        set_uvfile_path_format("/data1/logs/uvfilesaas/%(datatype)s/uvfile_%(yyyymmdd)s.log")
        for i in [4,]:
            # TongjiCommands.app_commonStore_bydaily(i, "jh_10a0e81221095bdba91f7688941948a6", "BIQU_ANDROID", "android", True)
            # TongjiCommands.app_commonStore_bydaily(i, "caiyu", "caiyu_ios_free", "ios", True)
            TongjiCommands.appointStore_bydaily(i, "caiyu", "biqu", "ios", "overall", "app", True)

            # TongjiCommands.app_appointStore_bydaily(i, "jh_10a0e81221095bdba91f7688941948a6", "biqu", "ios", True, ["store_remain"])
            # TongjiCommands.app_appointStore_bydaily(i, "jh_10a0e81221095bdba91f7688941948a6", "BIQU_ANDROID", "android", True, ["store_remain"])
        # TongjiCommands.app_commonStore_byhour("2016092719", "jh_48645aca02da76c6658b8d3e6d816b8e", "hbtv", "test", True)
        # TongjiCommands.app_defineTableStore(i, "jh_48645aca02da76c6658b8d3e6d816b8e", "hbtv", "test", True)
        # TongjiCommands.h5_defineTableStore(i, "jh_10a0e81221095bdba91f7688941948a6", "BQ_H5", "test", True)
        # TongjiCommands.h5_commonStore_bydaily(i, "jh_10a0e81221095bdba91f7688941948a6", "BQ_H5", "test", True)
        # TongjiCommands.h5_commonStore_byhour("2016092819", "jh_10a0e81221095bdba91f7688941948a6", "BQ_H5", "test", ifdel=True, funcnames=["storeAction"])

    if "defineTableStore" in sys.argv:
        set_log_path_format("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/????.log.gz")
        set_log_path_format_full("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log.gz")
        set_uvfile_path_format("/data1/logs/uvfilesaas/%(datatype)s/uvfile_%(yyyymmdd)s.log")
        data = GetAppkey_saas()
        for datatype in data:
            for item in data[datatype]:
                dbname, plat = item
                TongjiCommands.store_udf(1, dbname, datatype, plat)
        # store H5 log eventid
        data = GetAppkey_h5()
        for datatype in data:
            # if datatype != "ncf_360":
            #     continue
            for item in data[datatype]:
                dbname, plat = item
                TongjiCommands.store_udf(2, dbname, datatype, plat, tasks=["storeh5_eventid_pageid", "store_mapmeta"])

    if "defineTableStore_test" in sys.argv:
        set_log_path_format("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/????.log.gz")
        set_log_path_format_full("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log.gz")
        set_uvfile_path_format("/data1/logs/uvfilesaas/%(datatype)s/uvfile_%(yyyymmdd)s.log")
        data = GetAppkey_saas()
        for datatype in data:
            for item in data[datatype]:
                dbname, plat = item
                if datatype not in ["ncf_360"]:
                    continue
                print "-"*100
                print dbname, datatype, plat
                TongjiCommands.store_udf(2, dbname, datatype, plat, tasks=["storeh5_eventid_pageid", "store_mapmeta"])
            # TongjiCommands.store_udf(1, dbname, datatype, plat)
        # store H5 log eventid
        # data = GetAppkey_h5()
        # for datatype in data:
        #     dbname, plat = data[datatype]
        #     TongjiCommands.h5_defineTableStore(1, dbname, datatype, plat)

    if "defineTableStore_h5" in sys.argv:
        set_log_path_format("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/????.log.gz")
        set_log_path_format_full("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log.gz")
        set_uvfile_path_format("/data1/logs/uvfilesaas/%(datatype)s/uvfile_%(yyyymmdd)s.log")
        data = GetAppkey_h5()
        for datatype in data:
            # if datatype != "ncf_360":
            #     continue
            for item in data[datatype]:
                dbname, plat = item
                TongjiCommands.h5_defineTableStore(1, dbname, datatype, plat)

    if "store" in sys.argv:
        data = GetAppkey_saas()
        set_log_path_format("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/????.log.gz")
        set_log_path_format_full("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log.gz")
        set_uvfile_path_format("/data1/logs/uvfilesaas/%(datatype)s/uvfile_%(yyyymmdd)s.log")
        for i in range(1, 2):
            for datatype in data:
                for item in data[datatype]:
                    dbname, plat = item
                    print dbname, plat
                    if dbname != "testdb":
                        continue
                    print(datatype, dbname, plat)
                    TongjiCommands.appointStore_bydaily(i, dbname, datatype, plat, "market_remain", "app", True)

    if "week_saas" in sys.argv:
        data = GetAppkey_saas()
        set_log_path_format("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/????.log.gz")
        set_log_path_format_full("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log.gz")
        set_uvfile_path_format("/data1/logs/uvfilesaas/%(datatype)s/uvfile_%(yyyymmdd)s.log")
        for datatype in data:
            for item in data[datatype]:
                dbname, plat = item
                TongjiCommands.app_commonStore_byweek(1, dbname, datatype, plat, True)

    if "daily_saas" in sys.argv:
        data = GetAppkey_saas()
        set_log_path_format("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/????.log.gz")
        set_log_path_format_full("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log.gz")
        set_uvfile_path_format("/data1/logs/uvfilesaas/%(datatype)s/uvfile_%(yyyymmdd)s.log")
        for datatype in data:
            for item in data[datatype]:
                dbname, plat = item
                TongjiCommands.app_commonStore_bydaily(1, dbname, datatype, plat, False)

    if "daily_saas_test" in sys.argv:
        a = time.time()
        data = GetAppkey_saas()
        set_log_path_format("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/????.log.gz")
        set_log_path_format_full("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log.gz")
        set_uvfile_path_format("/data1/logs/uvfilesaas/%(datatype)s/uvfile_%(yyyymmdd)s.log")
        for datatype in data:
            if datatype != "caiyu_ad":
                continue
            for item in data[datatype]:
                dbname, plat = item
                TongjiCommands.app_commonStore_bydaily(1, dbname, datatype, plat, True)
        print time.time() - a

    if "store_saas" in sys.argv:
        data = GetAppkey_saas()
        set_log_path_format("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/????.log.gz")
        set_log_path_format_full("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log.gz")
        set_uvfile_path_format("/data1/logs/uvfilesaas/%(datatype)s/uvfile_%(yyyymmdd)s.log")
        days = [i for i in range(1, 2)]
        days.reverse()
        for i in days:
            for datatype in data:
                for item in data[datatype]:
                    dbname, plat = item
                    TongjiCommands.app_commonStore_bydaily(i, dbname, datatype, plat, True)

    if "daily_h5_saas" in sys.argv:
        # data = GetAppkey_h5()
        data = GetAppkeys(plat=["h5"], mongo_id=mongo_id)
        set_log_path_format("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/????.log.gz")
        set_log_path_format_full("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log.gz")
        set_log_path_format_full("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log.gz")
        set_uvfile_path_format("/data1/logs/uvfilesaas/%(datatype)s/uvfile_%(yyyymmdd)s.log")
        for datatype in data:
            for item in data[datatype]:
                dbname, plat = item
                TongjiCommands.h5_commonStore_bydaily(1, dbname, datatype, plat, True)

    if "daily_saas_appoint" in sys.argv:

        data = GetAppkey_saas()
        set_log_path_format("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/????.log.gz")
        set_log_path_format_full("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log.gz")
        set_uvfile_path_format("/data1/logs/uvfilesaas/%(datatype)s/uvfile_%(yyyymmdd)s.log")
        for datatype in data:
            if datatype != "caiyu_ad":
                continue
            for item in data[datatype]:
                dbname, plat = item
                # num, dbname, datatype, plat, appoint, apptype, ifdel=True, ifclearresult=True
                TongjiCommands.appointStore_bydaily(1, dbname, datatype, plat, "user_conf_udf", "app", ifdel=True)

    if "daily_h5_saas_appoint" in sys.argv:
        data = GetAppkeys(plat=["h5"], mongo_id=mongo_id)
        print data
        set_log_path_format("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/????.log.gz")
        set_log_path_format_full("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log.gz")
        set_uvfile_path_format("/data1/logs/uvfilesaas/%(datatype)s/uvfile_%(yyyymmdd)s.log")
        for datatype in data:
            for item in data[datatype]:
                dbname, plat = item
                if dbname == "ncf":
                    continue
                # num, dbname, datatype, plat, appoint, apptype, ifdel=True, ifclearresult=True
                TongjiCommands.appointStore_bydaily(1, dbname, datatype, plat, "page", "h5", ifdel=True)

    if "hour_h5_saas" in sys.argv:
        # data = GetAppkey_h5()
        data = GetAppkeys(plat=["h5"], mongo_id=mongo_id)
        set_log_path_format("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/????.log.gz")
        set_log_path_format_full("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log.gz")
        set_uvfile_path_format("/data1/logs/uvfilesaas/%(datatype)s/uvfile_%(yyyymmdd)s.log")
        delay_hour = 1
        yyyymmddhh = time.strftime("%Y%m%d%H", time.localtime(time.time()-60*60*delay_hour))
        for datatype in data:
            for item in data[datatype]:
                dbname, plat = item
                TongjiCommands.h5_commonStore_byhour(yyyymmddhh, dbname, datatype, plat, True)

    if "hour_h5_saas_store" in sys.argv:
        data = GetAppkey_h5()
        set_log_path_format("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/????.log.gz")
        set_log_path_format_full("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log.gz")
        set_uvfile_path_format("/data1/logs/uvfilesaas/%(datatype)s/uvfile_%(yyyymmdd)s.log")
        start_tm = time.mktime(time.strptime('2017-05-18+00:00:00', '%Y-%m-%d+%H:%M:%S'))
        end_tm = time.mktime(time.strptime('2017-05-18+11:00:00', '%Y-%m-%d+%H:%M:%S'))
        while start_tm <= end_tm:
            yyyymmddhhmm = time.strftime("%Y%m%d%H%M", time.localtime(start_tm))
            yyyymmddhh = time.strftime("%Y%m%d%H", time.localtime(start_tm))
            for datatype in data:
                if datatype not in ["ncf_ws"]:
                    continue
                for item in data[datatype]:
                    dbname, plat = item
                    try:
                        if yyyymmddhhmm.endswith("59"):
                            TongjiCommands.h5_commonStore_byhour(yyyymmddhh, dbname, datatype, plat, True)
                    except:
                        import traceback
                        print traceback.print_exc()
            start_tm += 60

    if "daily" in sys.argv:
        data = GetAppkey()
        for datatype in data:
            dbname, plat = data[datatype]
            TongjiCommands.app_commonStore_bydaily(1, dbname, datatype, plat)

