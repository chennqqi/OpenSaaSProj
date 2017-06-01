# -*- coding: utf-8 -*-
import sys
import time
from Tongji.TongjiCommands import TongjiCommands
from Commands import Commands

from DBClient.MysqlClient import MysqlClient
from SaaSConfig.config import set_log_path_format
from SaaSConfig.config import set_log_path_format_full
from SaaSConfig.config import set_uvfile_path_format
from SaaSConfig.config import mongo_ids


def get_datatype_saas():
    client = MysqlClient("saas_meta")
    result = [item for item in client.getAppkey()]
    client.closeMysql()
    return result

def get_datatype_saas_h5():
    client = MysqlClient("saas_meta")
    result = [item for item in client.getAppkey_h5()]
    client.closeMysql()
    return result

def GetAppkeys(**kwargs):
    client = MysqlClient("saas_server")
    result = client.getAppkey_kwargs(**kwargs)
    client.closeMysql()
    return result


if __name__ == "__main__":

    mongo_id = mongo_ids

    if "saas_rt" in sys.argv:
        if len(sys.argv) == 2:
            delay = 30
            start_tm = time.time() - delay * 60
        elif len(sys.argv) >= 3:
            try:
                start_tm = int(sys.argv[2])
            except:
                import traceback
                print traceback.print_exc()
                exit(0)
        yyyymmddhh = time.strftime("%Y%m%d%H", time.localtime(start_tm))
        yyyymmddhhmm = time.strftime("%Y%m%d%H%M", time.localtime(start_tm))
        set_log_path_format("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/????.log.gz")
        set_log_path_format_full("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log.gz")
        set_uvfile_path_format("/data1/logs/uvfilesaas/%(datatype)s/uvfile_%(yyyymmdd)s.log")
        saas_meta = GetAppkeys(plat=["android", "ios", "all", "feeling"], mongo_id=mongo_id)
        print saas_meta
        # exit(0)
        for item in saas_meta:
            dbname = item["cdkey"]
            datatype = item["appkey"]
            plat = item["plat"]
            Commands.commonStoreByMinute(datatype, start_tm, apptype="app")
            try:
                if yyyymmddhhmm.endswith("59"):
                    TongjiCommands.app_commonStore_byhour(yyyymmddhh, dbname, datatype, plat, ifdel=True)
            except:
                import traceback
                print traceback.print_exc()
        try:
            Commands.overallStoreByMinute(start_tm, mongo_id=mongo_id)
        except:
            import traceback
            print traceback.print_exc()

    if "saas_h5_rt" in sys.argv:
        if len(sys.argv) == 2:
            delay = 30
            start_tm = time.time() - delay * 60
        elif len(sys.argv) >= 3:
            try:
                start_tm = int(sys.argv[2])
            except:
                import traceback
                print traceback.print_exc()
                exit(0)
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
                Commands.commonStoreByMinute(datatype, start_tm, apptype="h5")
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
                Commands.commonStoreByDaily(datatype, 1, apptype="app")
            except:
                import traceback
                print(traceback.print_exc())
        saas_meta = GetAppkeys(plat=["h5"], mongo_id=mongo_id)
        print saas_meta
        appkeys = [item["appkey"] for item in saas_meta]
        # H5每日例行
        for datatype in appkeys:
            try:
                Commands.commonStoreByDaily(datatype, 1, apptype="h5")
            except:
                import traceback
                print(traceback.print_exc())

    if "test_all" in sys.argv:

        set_log_path_format("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/????.log.gz")
        set_log_path_format_full("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log.gz")
        set_uvfile_path_format("/data1/logs/uvfilesaas/%(datatype)s/uvfile_%(yyyymmdd)s.log")
        saas_meta = get_datatype_saas()
        yyyymmddhh = '2016111111'
        for item in saas_meta:
            dbname = item[0]
            datatype = item[1]
            plat = item[2]
            TongjiCommands.app_commonStore_byhour(yyyymmddhh, dbname, datatype, plat)


    if "test_uvlog" in sys.argv:
        set_log_path_format("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/????.log.gz")
        set_log_path_format_full("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log.gz")
        set_uvfile_path_format("/data1/logs/uvfilesaas/%(datatype)s/uvfile_%(yyyymmdd)s.log")
        # app每日例行
        saas_meta = get_datatype_saas()
        appkeys = ["caiyu_ios_free"]
        for datatype in appkeys:
            try:
                Commands.commonStoreByDaily(datatype, 1, apptype="app")
            except:
                import traceback
                print(traceback.print_exc())
        # saas_meta = get_datatype_saas_h5()
        # appkeys = [item[1] for item in saas_meta]
        # # H5每日例行
        # for datatype in appkeys:
        #     try:
        #         Commands.appointModeStoreByDaily(datatype, 1, "UVFile")
        #     except:
        #         import traceback
        #         print(traceback.print_exc())

    if "test_UserProfile" in sys.argv:
        delay = 15
        timestamp = time.time() - delay*60
        set_log_path_format("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/????.log.gz")
        set_log_path_format_full("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log.gz")
        set_uvfile_path_format("/data1/logs/uvfilesaas/%(datatype)s/uvfile_%(yyyymmdd)s.log")
        # app每日例行
        saas_meta = get_datatype_saas()
        appkeys = ["biqu"]
        for datatype in appkeys:
            try:
                Commands.appointModeStoreByMinute(datatype, 1475808795, "UserProfile")
            except:
                import traceback
                print(traceback.print_exc())

    if "guaeng_rt" in sys.argv:
        delay = 15
        start_tm = time.time() - delay*60
        set_log_path_format("/data1/logs/transformsaaslog/%(datatype)s/%(yyyymmdd)s/????.log.gz")
        set_log_path_format_full("/data1/logs/transformsaaslog/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log.gz")
        set_uvfile_path_format("/data1/logs/uvfile/%(datatype)s/uvfile_%(yyyymmdd)s.log")
        for datatype in ["guaeng"]:
            Commands.commonStoreByMinute(datatype, start_tm)

    if "guaeng" in sys.argv:
        delay = 30
        start_tm = time.time() - delay*60
        yyyymmddhh = time.strftime("%Y%m%d%H", time.localtime(start_tm))
        yyyymmddhhmm = time.strftime("%Y%m%d%H%M", time.localtime(start_tm))
        set_log_path_format("/data1/logs/transform/%(datatype)s/%(yyyymmdd)s/????.log")
        set_log_path_format_full("/data1/logs/transform/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log")
        set_uvfile_path_format("/data1/logs/uvfile/%(datatype)s/uvfile_%(yyyymmdd)s.log")
        for item in ["guaeng"]:
            datatype = item
            Commands.commonStoreByMinute(datatype, start_tm, apptype = "app")

    if "saas_rt_test" in sys.argv:
        start_tm = 1477928460
        datatype = "testlog"
        yyyymmddhh = time.strftime("%Y%m%d%H", time.localtime(start_tm))
        yyyymmddhhmm = time.strftime("%Y%m%d%H%M", time.localtime(start_tm))
        print yyyymmddhhmm
        set_log_path_format("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/????.log.gz")
        set_log_path_format_full("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log.gz")
        set_uvfile_path_format("/data1/logs/uvfilesaas/%(datatype)s/uvfile_%(yyyymmdd)s.log")
        Commands.commonStoreByMinute(datatype, start_tm, apptype="app")


    if "saas_daily_store" in sys.argv:
        set_log_path_format("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/????.log.gz")
        set_log_path_format_full("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log.gz")
        set_uvfile_path_format("/data1/logs/uvfilesaas/%(datatype)s/uvfile_%(yyyymmdd)s.log")
        # app每日例行
        saas_meta = get_datatype_saas()
        appkeys = [item[1] for item in saas_meta]
        days = [i for i in range(1, 15)]
        days.sort(reverse=True)
        for i in days:
            for datatype in appkeys:
                if datatype not in ["BIQU_ANDROID", "biqu"]:
                    continue
                try:
                    Commands.commonStoreByDaily(datatype, max(days)-i, apptype = "app")
                except:
                    import traceback
                    print(traceback.print_exc())
        # saas_meta = get_datatype_saas_h5()
        # appkeys = [item[1] for item in saas_meta]
        # # H5每日例行
        # for i in [2, 1]:
        #     for datatype in appkeys:
        #         try:
        #             Commands.commonStoreByDaily(datatype, i, apptype = "h5")
        #         except:
        #             import traceback
        #             print(traceback.print_exc())

    if "saas_daily_test" in sys.argv:
        set_log_path_format("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/????.log.gz")
        set_log_path_format_full("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log.gz")
        set_uvfile_path_format("/data1/logs/uvfilesaas/%(datatype)s/uvfile_%(yyyymmdd)s.log")
        # app每日例行
        # saas_meta = get_datatype_saas()
        saas_meta = get_datatype_saas_h5()
        appkeys = [item[1] for item in saas_meta]
        for datatype in appkeys:
            if datatype != "BQ_H5":
                continue
            try:
                Commands.commonStoreByDaily(datatype, 1, apptype = "h5")
            except:
                import traceback
                print(traceback.print_exc())


    if "rt_store" in sys.argv:
        set_log_path_format("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/????.log.gz")
        set_log_path_format_full("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log.gz")
        set_uvfile_path_format("/data1/logs/uvfilesaas/%(datatype)s/uvfile_%(yyyymmdd)s.log")
        start_tm = '2016-10-06+00:00:00'
        end_tm = '2016-10-07+00:00:00'
        saas_meta = get_datatype_saas()
        for item in saas_meta:
            print(item)
            dbname = item[0]
            datatype = item[1]
            if datatype != "BIQU_ANDROID":
                continue
            plat = item[2]
            Commands.dataRestore(start_tm, end_tm, dbname, datatype, plat)


    if "saas_rt_store" in sys.argv:
        set_log_path_format("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/????.log.gz")
        set_log_path_format_full("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log.gz")
        set_uvfile_path_format("/data1/logs/uvfilesaas/%(datatype)s/uvfile_%(yyyymmdd)s.log")
        start_tm = time.mktime(time.strptime('2016-09-25+00:00:00', '%Y-%m-%d+%H:%M:%S'))
        end_tm = time.mktime(time.strptime('2016-10-10+23:59:00', '%Y-%m-%d+%H:%M:%S'))
        now_stamp = time.time()
        saas_meta = get_datatype_saas()
        while start_tm <= end_tm:
            yyyymmddhhmm = time.strftime("%Y%m%d%H%M", time.localtime(start_tm))
            yyyymmddhh = time.strftime("%Y%m%d%H", time.localtime(start_tm))
            for item in saas_meta:
                dbname = item[0]
                datatype = item[1]
                plat = item[2]
                if datatype not in ["BIQU_ANDROID", "biqu"]:
                    continue
                Commands.commonStoreByMinute(datatype, start_tm, apptype="app")
                # try:
                #     if yyyymmddhhmm.endswith("59"):
                #         TongjiCommands.app_commonStore_byhour(yyyymmddhh, dbname, datatype, plat)
                # except:
                #     import traceback
                #     print traceback.print_exc()
                if yyyymmddhhmm.endswith("2359"):
                    from Tongji.TongjiCommands import TongjiCommands
                    import datetime
                    i = (datetime.datetime.today() - datetime.datetime.strptime(yyyymmddhhmm[:8], "%Y%m%d")).days
                    # Commands.appointModeStoreByDaily(datatype, i, "UVFile", apptype="app")
                    Commands.commonStoreByDaily(datatype, i, apptype="app")
                    TongjiCommands.app_commonStore_bydaily(i, dbname, datatype, plat, ifdel=False)
            start_tm += 60

    if "gueng_store" in sys.argv:
        set_log_path_format("/data1/logs/transform/%(datatype)s/%(yyyymmdd)s/????.log")
        set_log_path_format_full("/data1/logs/transform/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log")
        set_uvfile_path_format("/data1/logs/uvfile/%(datatype)s/uvfile_%(yyyymmdd)s.log")
        start_tm = time.mktime(time.strptime('2016-12-16+18:50:00', '%Y-%m-%d+%H:%M:%S'))
        end_tm = time.mktime(time.strptime('2016-12-16+18:50:00', '%Y-%m-%d+%H:%M:%S'))
        for item in ["guaeng"]:
            while start_tm <= end_tm:
                yyyymmddhhmm = time.strftime("%Y%m%d%H%M", time.localtime(start_tm))
                yyyymmddhh = time.strftime("%Y%m%d%H", time.localtime(start_tm))
                datatype = item
                Commands.appointModeStoreByMinute(datatype, start_tm, "UserProfile")
                start_tm += 60

    if "create_uvlog" in sys.argv:
        set_log_path_format("/data1/logs/transform/%(datatype)s/%(yyyymmdd)s/????.log")
        set_log_path_format_full("/data1/logs/transform/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log")
        set_uvfile_path_format("/data1/logs/uvfiletmp/%(datatype)s/uvfile_%(yyyymmdd)s.log")
        for datatype in ["guaengdemo"]:
            for i in range(1, 2):
                Commands.appointModeStoreByDaily(datatype, i, "UVFile", apptype = "app")

