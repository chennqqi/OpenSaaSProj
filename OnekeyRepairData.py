# -*- coding: utf-8 -*-
from SaaSConfig.config import set_log_path_format
from SaaSConfig.config import set_log_path_format_full
from SaaSConfig.config import set_uvfile_path_format
from Tongji.TongjiCommands import TongjiCommands
from Commands import Commands
from DBClient.MysqlClient import MysqlClient
import time
import datetime
import sys


def get_datatype_saas():
    client = MysqlClient("saas_meta")
    result = [[item["cdkey"], item["appkey"], item["plat"]] for item in client.getAppkey_kwargs(plat = ["android", "ios", "all", "feeling"])]
    client.closeMysql()
    return result


def get_datatype_saas_h5():
    client = MysqlClient("saas_meta")
    result = [[item["cdkey"], item["appkey"], item["plat"]] for item in client.getAppkey_kwargs(plat = ["h5"])]
    client.closeMysql()
    return result


if __name__ == "__main__":

    if "tongji" in sys.argv:
        set_log_path_format("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/????.log.gz")
        set_log_path_format_full("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log.gz")
        set_uvfile_path_format("/data1/logs/uvfilesaas/%(datatype)s/uvfile_%(yyyymmdd)s.log")
        # 重跑入库数据
        saas_meta = get_datatype_saas()
        for item in saas_meta:
            dbname = item[0]
            datatype = item[1]
            plat = item[2]
            try:
                TongjiCommands.app_commonStore_bydaily(1, dbname, datatype, plat, ifdel=True)
            except:
                import traceback
                print(traceback.print_exc())

    if "appoint_tongji" in sys.argv:
        set_log_path_format("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/????.log.gz")
        set_log_path_format_full("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log.gz")
        set_uvfile_path_format("/data1/logs/uvfilesaas/%(datatype)s/uvfile_%(yyyymmdd)s.log")
        saas_meta = get_datatype_saas()
        days = [i for i in range(1, 40)]
        days.remove()
        for i in range(1, 40):
            for item in saas_meta:
                dbname = item[0]
                datatype = item[1]
                plat = item[2]
                # if datatype not in ["BIQU_ANDROID", "biqu"]:
                #     continue
                TongjiCommands.app_appointStore_bydaily(23-i, dbname, datatype, plat, True, ["store_overall_week"])
                # TongjiCommands.app_commonStore_byweek(4 - i, dbname, datatype, plat, True, ["store_overall_week"])

    if "create_uvlog" in sys.argv:
        set_log_path_format("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/????.log.gz")
        set_log_path_format_full("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log.gz")
        set_uvfile_path_format("/data1/logs/uvfilesaas/%(datatype)s/uvfile_%(yyyymmdd)s.log")
        for datatype in ["BIQU_ANDROID"]:
            for i in [17]:
                Commands.appointModeStoreByDaily(datatype, i, "UVFile", apptype = "app")

    if "store_mode_daily" in sys.argv:
        set_log_path_format("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/????.log.gz")
        set_log_path_format_full("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log.gz")
        set_uvfile_path_format("/data1/logs/uvfilesaas/%(datatype)s/uvfile_%(yyyymmdd)s.log")
        for i in range():
            Commands.commonStoreByDaily("caiyu_ad", 1, apptype="app")

    if "run" in sys.argv:
        set_log_path_format("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/????.log.gz")
        set_log_path_format_full("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log.gz")
        set_uvfile_path_format("/data1/logs/uvfilesaas/%(datatype)s/uvfile_%(yyyymmdd)s.log")

        # set_log_path_format("/data1/logs/transform/%(datatype)s/%(yyyymmdd)s/????.log")
        # set_log_path_format_full("/data1/logs/transform/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log")
        # set_uvfile_path_format("/data1/logs/uvfile/%(datatype)s/uvfile_%(yyyymmdd)s.log")

        start_tm = time.mktime(time.strptime('2017-03-27+00:00:00', '%Y-%m-%d+%H:%M:%S'))
        # start_tm = time.mktime(time.strptime('2017-03-26+12:00:00', '%Y-%m-%d+%H:%M:%S'))
        end_tm = time.mktime(time.strptime('2017-03-27+23:59:00', '%Y-%m-%d+%H:%M:%S'))
        # end_tm = time.mktime(time.strptime('2017-03-27+14:50:00', '%Y-%m-%d+%H:%M:%S'))
        saas_meta = get_datatype_saas()
        # saas_meta = [["guaeng", "huiyue_ios", "ios"], ["guaeng", "huiyue_ad", "android"]]
        while start_tm <= end_tm:
            yyyymmddhhmm = time.strftime("%Y%m%d%H%M", time.localtime(start_tm))
            yyyymmddhh = time.strftime("%Y%m%d%H", time.localtime(start_tm))
            for item in saas_meta:
                dbname = item[0]
                datatype = item[1]
                # if dbname != "jh_10a0e81221095bdba91f7688941948a6": # biqu
                #     continue
                if datatype not in ["caiyu_ios_free", "caiyu_ad"]:
                    continue
                plat = item[2]
                # if datatype not in ["hbtv", "caiyu_ad", "caiyu_ad_bankExt", "caiyu_ad_houseFundExt"]:
                #     continue
                # 重跑实时数据
                # 每分钟例行
                Commands.commonStoreByMinute(datatype, start_tm, apptype="app")
                # 每小时例行
                try:
                    if yyyymmddhhmm.endswith("59"):
                        TongjiCommands.app_commonStore_byhour(yyyymmddhh, dbname, datatype, plat)
                except:
                    import traceback
                    print traceback.print_exc()
                # if yyyymmddhhmm.endswith("2359"):
                #     from Tongji.TongjiCommands import TongjiCommands
                #     i = (datetime.datetime.today() - datetime.datetime.strptime(yyyymmddhhmm[:8], "%Y%m%d")).days
                #     # Commands.appointModeStoreByDaily(datatype, i, "UVFile", apptype="app")
                #     # 重跑按天数据(UVFile)
                #     try:
                #         Commands.commonStoreByDaily(datatype, i, apptype="app")
                #         # Commands.appointModeStoreByDaily(datatype, i, "UserActive")
                #     except:
                #         import traceback
                #         print(traceback.print_exc())
                #     # 重跑入库数据
                #     try:
                #         TongjiCommands.app_commonStore_bydaily(i, dbname, datatype, plat, ifdel=True)
                #     except:
                #         import traceback
                #         print(traceback.print_exc())
                #     # 重跑上周数据
                #     try:
                #         week_day = datetime.datetime.strptime(yyyymmddhh[:8], "%Y%m%d").weekday()
                #         if week_day == 3:
                #             TongjiCommands.app_commonStore_byweek(1, dbname, datatype, plat, ifdel=True)
                #     except:
                #         import traceback
                #         print(traceback.print_exc())
            start_tm += 60

    if "runh5" in sys.argv:
        set_log_path_format("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/????.log.gz")
        set_log_path_format_full("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log.gz")
        set_uvfile_path_format("/data1/logs/uvfilesaas/%(datatype)s/uvfile_%(yyyymmdd)s.log")

        # set_log_path_format("/data1/logs/transform/%(datatype)s/%(yyyymmdd)s/????.log")
        # set_log_path_format_full("/data1/logs/transform/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log")
        # set_uvfile_path_format("/data1/logs/uvfile/%(datatype)s/uvfile_%(yyyymmdd)s.log")

        start_tm = time.mktime(time.strptime('2017-03-24+16:16:00', '%Y-%m-%d+%H:%M:%S'))
        end_tm = time.mktime(time.strptime('2017-03-26+14:16:00', '%Y-%m-%d+%H:%M:%S'))
        # saas_meta = get_datatype_saas()
        saas_meta = [["jinjiedao", "jinjd_h5", "h5"], ["ncf", "ncf_360", "h5"], ["ncf", "ncf_ws", "h5"], ["ncf", "ncf_h5", "h5"], ["jh_10a0e81221095bdba91f7688941948a6", "BQ_H5", "h5"], ]
        while start_tm <= end_tm:
            yyyymmddhhmm = time.strftime("%Y%m%d%H%M", time.localtime(start_tm))
            yyyymmddhh = time.strftime("%Y%m%d%H", time.localtime(start_tm))
            for item in saas_meta:
                dbname = item[0]
                datatype = item[1]
                plat = item[2]
                # if datatype not in ["hbtv", "caiyu_ad", "caiyu_ad_bankExt", "caiyu_ad_houseFundExt"]:
                #     continue
                # # 重跑实时数据
                # # 每分钟例行
                Commands.commonStoreByMinute(datatype, start_tm, apptype="h5")
                # 每小时例行
                try:
                    if yyyymmddhhmm.endswith("59"):
                        TongjiCommands.h5_commonStore_byhour(yyyymmddhh, dbname, datatype, plat, ifdel=True)
                except:
                    import traceback
                    print traceback.print_exc()
                if yyyymmddhhmm.endswith("2359"):
                    from Tongji.TongjiCommands import TongjiCommands
                    i = (datetime.datetime.today() - datetime.datetime.strptime(yyyymmddhhmm[:8], "%Y%m%d")).days
                    # 重跑按天数据
                    try:
                        Commands.commonStoreByDaily(datatype, i, apptype="h5")
                    except:
                        import traceback
                        print(traceback.print_exc())
                    # # 重跑入库数据
                    try:
                        TongjiCommands.h5_commonStore_bydaily(i, dbname, datatype, plat, ifdel=True)
                    except:
                        import traceback
                        print(traceback.print_exc())
            start_tm += 60


    if "test" in sys.argv:
        print get_datatype_saas()

    if "runappoint" in sys.argv:
        # set_log_path_format("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/????.log.gz")
        # set_log_path_format_full("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log.gz")
        # set_uvfile_path_format("/data1/logs/uvfilesaas/%(datatype)s/uvfile_%(yyyymmdd)s.log")

        set_log_path_format("/data1/logs/transform/%(datatype)s/%(yyyymmdd)s/????.log")
        set_log_path_format_full("/data1/logs/transform/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log")
        set_uvfile_path_format("/data1/logs/uvfile/%(datatype)s/uvfile_%(yyyymmdd)s.log")

        start_tm = time.mktime(time.strptime('2016-10-01+00:00:00', '%Y-%m-%d+%H:%M:%S'))
        end_tm = time.mktime(time.strptime('2016-11-15+21:00:00', '%Y-%m-%d+%H:%M:%S'))
        saas_meta = get_datatype_saas()
        while start_tm <= end_tm:
            yyyymmddhhmm = time.strftime("%Y%m%d%H%M", time.localtime(start_tm))
            yyyymmddhh = time.strftime("%Y%m%d%H", time.localtime(start_tm))
            for item in saas_meta:
                dbname = item[0]
                datatype = item[1]
                plat = item[2]
                # if datatype not in ["feeling"]:
                #     continue
                # # 重跑实时数据
                # # 每分钟例行
                # Commands.commonStoreByMinute(datatype, start_tm, apptype="app")
                # Commands.appointModeStoreByMinute(datatype, start_tm, "UserProfile")
                # Commands.appointModeStoreByMinute(datatype, start_tm, "UserCrumbs")
                Commands.appointModeStoreByMinute(datatype, start_tm, "UserCrumbs")
            start_tm += 60
