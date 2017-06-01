# -*- coding: utf-8 -*-
import __init__
from collections import OrderedDict
# from StoreCommon import StoreRT
from StoreCommon.StoreCommon import StoreCommon
from StoreCommon.StoreCommonH5 import StoreCommonH5
from StoreCommon.StoreUserDefineID import StoreUserDefineID
from StoreCommon.StoreUserDefineIDH5 import StoreUserDefineIDH5
from StoreCommon.StoreUserDefineMapMeta import StoreUserDefineMapMeta


# 对外提供功能调用接口
class TongjiCommands(object):

    global \
        app_store_byhour, \
        app_store_bydaily, \
        app_store_byweek, \
        h5_store_byhour, \
        h5_store_bydaily, \
        app_store_udfid, \
        app_funcs_udfid, \
        h5_store_udfid, \
        h5_funcs_udfid, \
        store_mapmeta_udfid, \
        mapmeta_funcs_udfid

    global commands, tasks

    commands = OrderedDict([])
    tasks = OrderedDict([])

    try:
        app_store_byhour = StoreCommon("hour")
        task_part = OrderedDict([
            ("overall_rt_byhour", (app_store_byhour.storeappoint, "overall_rt_byhour")),
            ("event_rt_byhour", (app_store_byhour.storeappoint, "event_rt_byhour")),
        ])
        tasks.update(**task_part)
    except:
        import traceback
        print(traceback.print_exc())

    try:
        app_store_bydaily = StoreCommon("daily")
        task_part = OrderedDict([
            ("overall", (app_store_byhour.storeappoint, "overall")),
            ("event", (app_store_byhour.storeappoint, "event")),
            ("page", (app_store_byhour.storeappoint, "page")),
            ("in_distribute", (app_store_byhour.storeappoint, "in_distribute")),
            ("usetime_distribute", (app_store_byhour.storeappoint, "usetime_distribute")),
            ("market_hy_7", (app_store_byhour.storeappoint, "market_hy_7")),
            ("market_remain", (app_store_byhour.storeappoint, "market_remain")),
            ("ua", (app_store_byhour.storeappoint, "ua")),
            ("os", (app_store_byhour.storeappoint, "os")),
            ("loc", (app_store_byhour.storeappoint, "loc")),
        ])
        tasks.update(**task_part)
    except:
        import traceback
        print(traceback.print_exc())

    try:
        app_store_byweek = StoreCommon("week")
        task_part = OrderedDict([
            ("overall_week", (app_store_byhour.storeappoint, "overall_week")),
        ])
        tasks.update(**task_part)
    except:
        import traceback
        print(traceback.print_exc())

    try:
        h5_store_byhour = StoreCommonH5("hour")
        task_part = OrderedDict([
            ("overall_rt_byhour_h5", (h5_store_byhour.storeappoint, "overall_rt_byhour")),
            ("page_rt_byhour_h5", (h5_store_byhour.storeappoint, "page_rt_byhour")),
            ("event_rt_byhour_h5", (h5_store_byhour.storeappoint, "event_rt_byhour")),
        ])
        tasks.update(**task_part)
    except:
        import traceback
        print(traceback.print_exc())

    try:
        h5_store_bydaily = StoreCommonH5("daily")
        task_part = OrderedDict([
            ("overall_h5", (h5_store_bydaily.storeappoint, "overall")),
            ("summary_h5", (h5_store_bydaily.storeappoint, "summary")),
            ("page_h5", (h5_store_bydaily.storeappoint, "page")),
            ("event_h5", (h5_store_bydaily.storeappoint, "event")),
            ("ref_source_h5", (h5_store_bydaily.storeappoint, "ref_source")),
        ])
        tasks.update(**task_part)
    except:
        import traceback
        print(traceback.print_exc())

    try:
        app_store_udfid = StoreUserDefineID()
        app_funcs_udfid = OrderedDict([
            ("store_eventid_pageid", app_store_udfid.store), # 即将废弃
            # ("store", app_store_udfid.store)
        ])
        commands.setdefault("storeapp_eventid_pageid", app_store_udfid.store)
    except:
        import traceback
        print(traceback.print_exc())

    try:
        h5_store_udfid = StoreUserDefineIDH5()
        h5_funcs_udfid = OrderedDict([
            ("store_eventid_pageid", h5_store_udfid.store), # 即将废弃
            # ("store", h5_store_udfid.store)
        ])
        commands.setdefault("storeh5_eventid_pageid", h5_store_udfid.store)
    except:
        import traceback
        print(traceback.print_exc())

    try:
        store_mapmeta_udfid = StoreUserDefineMapMeta()
        mapmeta_funcs_udfid = OrderedDict([
            ("store", store_mapmeta_udfid.store)
        ])
        commands.setdefault("store_mapmeta", store_mapmeta_udfid.store)
    except:
        import traceback
        print(traceback.print_exc())

    def __init__(self):
        pass

    @staticmethod
    def app_commonStore_byhour(yyyymmddhh, dbname, datatype, plat, ifdel=True, ifclearresult=True):
        global app_store_byhour
        app_store_byhour.storeByHour(yyyymmddhh, dbname, datatype, plat, ifdel, ifclearresult)

    @staticmethod
    def app_commonStore_bydaily(num, dbname, datatype, plat, ifdel=False, ifclearresult=True):
        global app_store_bydaily
        app_store_bydaily.storeByDaily(num, dbname, datatype, plat, ifdel, ifclearresult)

    @staticmethod
    def app_commonStore_byweek(num, dbname, datatype, plat, ifdel=False, ifclearresult=True):
        global app_store_byweek
        app_store_byweek.storeByWeek(num, dbname, datatype, plat, ifdel, ifclearresult)

    @staticmethod
    def appointStore_bydaily(num, dbname, datatype, plat, appoint, apptype, ifdel=True, ifclearresult=True):
        '''
        :param num:
        :param dbname:
        :param datatype:
        :param plat:
        :param appoint: 对应表名
        :param apptype:
        :param ifdel:
        :param ifclearresult:
        :return:
        '''
        if apptype == "app":
            global app_store_bydaily
            storer = app_store_bydaily
        elif apptype == "h5":
            global h5_store_bydaily
            storer = h5_store_bydaily
        storer.storeappointbydaily(num, dbname, datatype, plat, appoint, ifdel, ifclearresult)

    @staticmethod
    def appointStore_byhour(num, dbname, datatype, plat, appoint, apptype, ifdel=True, ifclearresult=True):
        '''
        :param num:
        :param dbname:
        :param datatype:
        :param plat:
        :param appoint:
        :param apptype:
        :param ifdel:
        :param ifclearresult:
        :return:
        '''
        storer = None
        if apptype == "app":
            global app_store_byhour
            storer = app_store_byhour
        elif apptype == "h5":
            global h5_store_byhour
            storer = h5_store_byhour
        if storer is None:
            return False
        storer.storeappointbyhour(num, dbname, datatype, plat, appoint, ifdel, ifclearresult)

    @staticmethod
    def h5_commonStore_byhour(yyyymmddhh, dbname, datatype, plat="h5", ifdel=False, ifclearresult=True):
        global h5_store_byhour
        h5_store_byhour.storeByHour(yyyymmddhh, dbname, datatype, plat, ifdel, ifclearresult)

    @staticmethod
    def h5_commonStore_bydaily(num, dbname, datatype, plat="h5", ifdel=False, ifclearresult=True):
        global h5_store_bydaily
        h5_store_bydaily.storeByDaily(num, dbname, datatype, plat, ifdel, ifclearresult)

    @staticmethod
    def store_udf(num, dbname, datatype, plat,
                  tasks = ["storeapp_eventid_pageid", "store_mapmeta"],
                  *args, **kwargs):
        global commands
        for taskname in tasks:
            try:
                exector = commands.get(taskname, None)
                if exector:
                    exector(num, dbname, datatype, plat)
                else:
                    print("Can't found %s exector."%taskname)
            except:
                import traceback
                print(traceback.print_exc())

    @staticmethod
    # store_mapmeta_udfid
    def app_defineTableStore(num, dbname, datatype, plat, ifdel=False, funcnames = app_funcs_udfid.keys()):
        global app_funcs_udfid
        for funcname in funcnames:
            try:
                function = app_funcs_udfid[funcname]
                function(num, dbname, datatype, plat)
            except:
                import traceback
                print(traceback.print_exc())

    @staticmethod
    def h5_defineTableStore(num, dbname, datatype, plat, ifdel=False, funcnames = h5_funcs_udfid.keys()):
        global h5_funcs_udfid
        for funcname in funcnames:
            try:
                function = h5_funcs_udfid[funcname]
                function(num, dbname, datatype, plat)
            except:
                import traceback
                print(traceback.print_exc())

if __name__ == "__main__":
    from SaaSConfig.config import set_uvfile_path_format
    from SaaSConfig.config import set_log_path_format_full
    from SaaSConfig.config import set_log_path_format
    set_log_path_format("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/????.log.gz")
    set_log_path_format_full("/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log.gz")
    set_uvfile_path_format("/data1/logs/uvfilesaas/%(datatype)s/uvfile_%(yyyymmdd)s.log")
    tester = TongjiCommands()
    days = [i for i in range(1, 2)]
    days.reverse()
    # num, dbname, datatype, plat, appoint, apptype, ifdel=True, ifclearresult=True
    for i in days:
        print(i)
        tester.appointStore_bydaily(i, "ncf", "ncf_h5", "h5", "summary", "h5")
