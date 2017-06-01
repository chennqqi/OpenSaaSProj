# -*- coding: utf-8 -*-
import sys
import time
from Tongji.TongjiCommands import TongjiCommands
from Commands import Commands

from DBClient.MysqlClient import MysqlClient
from SaaSConfig.config import set_log_path_format
from SaaSConfig.config import set_log_path_format_full
from SaaSConfig.config import set_uvfile_path_format

def GetAppkeys(**kwargs):
    client = MysqlClient("saas_server")
    result = client.getAppkey_kwargs(kwargs)
    return result

if __name__ == "__main__":

    mongo_id = 2

    if "saas_rt" in sys.argv:
        delay = 30
        start_tm = time.time() - delay*60
        yyyymmddhh = time.strftime("%Y%m%d%H", time.localtime(start_tm))
        yyyymmddhhmm = time.strftime("%Y%m%d%H%M", time.localtime(start_tm))
        set_log_path_format("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/????.log.gz")
        set_log_path_format_full("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log.gz")
        set_uvfile_path_format("/data1/logs/uvfilesaas/%(datatype)s/uvfile_%(yyyymmdd)s.log")
        saas_meta = GetAppkeys(plat=["android", "ios", "all", "feeling"], mongo_id=mongo_id)
        for item in saas_meta:
            dbname = item["cdkey"]
            datatype = item["appkey"]
            plat = item["plat"]
            Commands.commonStoreByMinute(datatype, start_tm, apptype = "app")
            try:
                if yyyymmddhhmm.endswith("59"):
                    TongjiCommands.app_commonStore_byhour(yyyymmddhh, dbname, datatype, plat)
            except:
                import traceback
                print traceback.print_exc()

    if "saas_h5_rt" in sys.argv:
        delay = 30
        start_tm = time.time() - delay*60
        yyyymmddhh = time.strftime("%Y%m%d%H", time.localtime(start_tm))
        yyyymmddhhmm = time.strftime("%Y%m%d%H%M", time.localtime(start_tm))

        set_log_path_format("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/????.log.gz")
        set_log_path_format_full("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log.gz")
        set_uvfile_path_format("/data1/logs/uvfilesaas/%(datatype)s/uvfile_%(yyyymmdd)s.log")
        saas_meta = GetAppkeys(plat=["h5"], mongo_id=mongo_id)
        appkeys = [item["appkey"] for item in saas_meta]
        try:
            for datatype in appkeys:
                print(datatype)
                Commands.commonStoreByMinute(datatype, start_tm, apptype = "h5")
        except:
            import traceback
            print(traceback.print_exc())

    if "saas_daily" in sys.argv:
        set_log_path_format("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/????.log.gz")
        set_log_path_format_full("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log.gz")
        set_uvfile_path_format("/data1/logs/uvfilesaas/%(datatype)s/uvfile_%(yyyymmdd)s.log")
        # app每日例行
        saas_meta = GetAppkeys(plat=["android", "ios", "all", "feeling"], mongo_id=mongo_id)
        appkeys = [item["appkey"] for item in saas_meta]
        for datatype in appkeys:
            try:
                Commands.commonStoreByDaily(datatype, 1, apptype = "app")
            except:
                import traceback
                print(traceback.print_exc())
        saas_meta = GetAppkeys(plat=["h5"], mongo_id=mongo_id)
        appkeys = [item[1] for item in saas_meta]
        # H5每日例行
        for datatype in appkeys:
            try:
                Commands.commonStoreByDaily(datatype, 1, apptype = "h5")
            except:
                import traceback
                print(traceback.print_exc())

