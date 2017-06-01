# -*- coding: utf-8 -*-
# import glob
import time
from collections import OrderedDict
# My tools
from SaaSCommon.JHDecorator import fn_timer
from LogController.UserCrumbs import UserCrumbs
from LogController.UserCrumbsH5 import UserCrumbsH5
from LogController.UserProfile import UserProfile
from LogController.UserProfileH5 import UserProfileH5
from LogController.UserEvent import UserEvent
from LogController.UserEventH5 import UserEventH5
from LogController.UVFile import UVFile
from LogController.UserMapMeta import UserMapMeta
from LogController.UserMapMetaH5 import UserMapMetaH5
# from LogController.UserActiveUpdate import UserActiveUpdate
from Tongji.TongjiCommands import TongjiCommands

# 实时入库(每分钟)
from SaaSStore.ModeWriterMongo import UserCrumbsWriter
from SaaSStore.ModeWriterMongo import UserProfileWriter
from SaaSStore.ModeWriterMongo import UserEventWriter
from SaaSStore.ModeWriterMongo import UserMapMetaWriter
from SaaSStore.ModeWriteMongoUserIP import ModeWriteMongoUserIP

# from SaaSStore.ModeWriterMongo import UserActiveUpdateWriter
# 每天入库
# from SaaSStore.ModeWriterMongo import UserActiveWriter
from SaaSStore.ModeWriterFile import UVFileWriter

# 引入数据库数据
from DBClient.MysqlClient import MysqlClient



class Commands(object):

    global mode_writer_contrast, tasks, tasks_overall

    # format: 任务名称（模型名称）、模型类、存储类
    mode_writer_contrast = OrderedDict([
        ("UserCrumbs", (UserCrumbs, UserCrumbsWriter)),
        ("UserCrumbsH5", (UserCrumbsH5, UserCrumbsWriter)),

        ("UserProfile", (UserProfile, UserProfileWriter)),
        ("UserProfileH5", (UserProfileH5, UserProfileWriter)),

        ("UserEvent", (UserEvent, UserEventWriter)),
        ("UserEventH5", (UserEventH5, UserEventWriter)),

        ("UserMapMeta", (UserMapMeta, UserMapMetaWriter)),
        ("UserMapMetaH5", (UserMapMetaH5, UserMapMetaWriter)),

        ("UserIP", (None, ModeWriteMongoUserIP)),

        # ("UserActiveUpdate", (UserActiveUpdate, UserActiveUpdateWriter)),

        ("UVFile", (UVFile, UVFileWriter)),
        # ("UserActive", (None, UserActiveWriter)),
    ])

    tasks = OrderedDict([
        # ("daily_app", ("UserActive", "UVFile")),
        ("daily_app", ("UVFile", )),
        # ("daily_h5", ("UVFile", )),
        # ("daily_h5", ("UserActive", )),
        # ("minute_app", ("UserActiveUpdate", "UserProfile", "UserCrumbs", "UserEvent", "UserMapMeta")),
        # ("minute_app", ("UserProfile", "UserCrumbs", "UserEvent", "UserMapMeta")),
        ("minute_app", ("UserProfile", "UserCrumbs", "UserMapMeta")),
        # ("minute_h5", ("UserProfileH5", "UserCrumbsH5", "UserEventH5", "UserMapMetaH5")),
        ("minute_h5", ("UserProfileH5", "UserCrumbsH5", "UserMapMetaH5")),
    ])

    tasks_overall = OrderedDict([
        ("minute_UserIP", ("UserIP",)),
    ])

    def __init__(self):
        pass

    @staticmethod
    @fn_timer
    def dataRestore(start_tm, end_tm, dbname, appkey, plat):
        if plat in set(["android", "ios"]):
            apptype = "app"
        elif plat in set(["h5"]):
            apptype = "h5"
        else:
            raise Exception
        start_timestamp = time.mktime(time.strptime(start_tm, '%Y-%m-%d+%H:%M:%S'))
        end_timestamp = time.mktime(time.strptime(end_tm, '%Y-%m-%d+%H:%M:%S'))
        while start_timestamp < end_timestamp:
            yyyymmddhhmm = time.strftime("%Y%m%d%H%M", time.localtime(start_timestamp))
            yyyymmddhh = time.strftime("%Y%m%d%H", time.localtime(start_timestamp))
            print(appkey, yyyymmddhhmm)
            Commands.commonStoreByMinute(appkey, start_timestamp, apptype=apptype)
            if yyyymmddhhmm.endswith("59"):
                TongjiCommands.app_commonStore_byhour(yyyymmddhh, dbname, appkey, plat)
            start_timestamp += 60

    # 执行每分钟例行任务
    @staticmethod
    @fn_timer
    def commonStoreByMinute(datatype, timestamp, *args, **kwargs):
        global mode_writer_contrast
        global tasks
        ct = time.strftime("%Y-%m-%d+%H%M", time.localtime(timestamp))
        yyyymmddhhmm = ct.replace("-", "").replace("+", "")
        yyyy_mm_dd, hhmm = ct.split("+")[0], ct.split("+")[1]
        apptype = kwargs["apptype"]
        for modename in tasks["".join(["minute", "_%s"%apptype])]:
            # print(modename, datatype, apptype)
            try:
                mode_class, store_class = mode_writer_contrast[modename]
                modecreator = mode_class(datatype, yyyy_mm_dd, hhmm, last=1) if mode_class else None
                result = modecreator.dataCollect() if modecreator else None
                # 获取datatype对应入库mongo编号
                m_client = MysqlClient("saas_server")
                mongo_ids = m_client.get_mongoid(datatype)
                print __name__, datatype, mongo_ids
                m_client.closeMysql()
                for mongo_id in mongo_ids:
                    try:
                        # print datatype, store_class, mode_class
                        storer = store_class(mongo_id)
                        if yyyymmddhhmm.endswith("0000"):
                            storer.remove(datatype, modename, yyyymmddhhmm[:8])
                            print("invoking remove interface, remove data or Nothing to do! ", datatype, modename, yyyymmddhhmm[:8])
                        storer.write(result, datatype, modename, modecreator, today=yyyy_mm_dd)
                    except:
                        import traceback
                        print(traceback.print_exc())
            except:
                import traceback
                print(traceback.print_exc())

    # 执行每分钟例行任务
    @staticmethod
    @fn_timer
    def overallStoreByMinute(timestamp, *args, **kwargs):
        global mode_writer_contrast
        global tasks_overall
        ct = time.strftime("%Y-%m-%d+%H%M", time.localtime(timestamp))
        yyyymmddhhmm = ct.replace("-", "").replace("+", "")
        yyyy_mm_dd, hhmm = ct.split("+")[0], ct.split("+")[1]
        for modename in tasks_overall["minute_UserIP"]:
            try:
                mode_class, store_class = mode_writer_contrast[modename]
                modecreator = mode_class(yyyy_mm_dd, hhmm, last=1) if mode_class else None
                result = modecreator.dataCollect() if modecreator else None
                mongo_ids = kwargs.get("mongo_id", [1, 2])
                for mongo_id in mongo_ids:
                    try:
                        storer = store_class(mongo_id)
                        if yyyymmddhhmm.endswith("0000"):
                            storer.remove(modename, yyyymmddhhmm[:8])
                            print("invoking remove interface, remove data or Nothing to do! ", modename,
                                  yyyymmddhhmm[:8])
                        storer.write(result, modename, modecreator, today=yyyy_mm_dd)
                    except:
                        import traceback
                        print(traceback.print_exc())
            except:
                import traceback
                print(traceback.print_exc())


    # 执行每天例行任务
    @staticmethod
    @fn_timer
    def commonStoreByDaily(datatype, num, *args, **kwargs):
        global mode_writer_contrast
        global tasks
        ct = time.strftime("%Y-%m-%d+%H%M", time.localtime(time.time()-86400*num))
        yyyy_mm_dd, hhmm = ct.split("+")[0], "2359"
        apptype = kwargs["apptype"]
        for modename in tasks["".join(["daily", "_%s"%apptype])]:
            print(modename, datatype, apptype)
            try:
                mode_class, store_class = mode_writer_contrast[modename]
                modecreator = mode_class(datatype, yyyy_mm_dd) if mode_class else None
                result = modecreator.dataCollect() if modecreator else None
                # 获取datatype对应入库mongo编号
                m_client = MysqlClient("saas_server")
                mongo_ids = m_client.get_mongoid(datatype)
                m_client.closeMysql()
                for mongo_id in mongo_ids[:1]:
                    storer = store_class(mongo_id)
                    storer.write(result, datatype, modename, modecreator, num=num, today=yyyy_mm_dd)
            except:
                import traceback
                print(traceback.print_exc())

    # 根据指定模型名称，执行某一个例行
    @staticmethod
    @fn_timer
    def appointModeStoreByMinute(datatype, timestamp, modename, *args, **kwargs):
        global mode_writer_contrast
        ct = time.strftime("%Y-%m-%d+%H%M", time.localtime(timestamp))
        yyyy_mm_dd, hhmm = ct.split("+")[0], ct.split("+")[1]
        mode_class, storer_class = mode_writer_contrast[modename]
        modecreator = mode_class(datatype, yyyy_mm_dd, hhmm, last=1) if mode_class else None
        result = modecreator.dataCollect() if modecreator else None
        m_client = MysqlClient("saas_server")
        mongo_ids = m_client.get_mongoid(datatype)
        m_client.closeMysql()
        for mongo_id in mongo_ids:
            try:
                storer = storer_class(mongo_id)
                ct = time.strftime("%Y-%m-%d+%H%M", time.localtime(timestamp))
                yyyymmddhhmm = ct.replace("-", "").replace("+", "")
                yyyy_mm_dd, hhmm = ct.split("+")[0], ct.split("+")[1]
                if yyyymmddhhmm.endswith("0000"):
                    storer.remove(datatype, modename, yyyymmddhhmm[:8])
                    print("invoking remove interface, remove data or Nothing to do! ", datatype, modename, yyyymmddhhmm[:8])
                storer.write(result, datatype, modename, modecreator, today=yyyy_mm_dd)
            except:
                import traceback
                print(traceback.print_exc())

    # 根据指定模型名称，执行某一个例行
    @staticmethod
    @fn_timer
    def appointModeStoreByDaily(datatype, num, modename, *args, **kwargs):
        global mode_writer_contrast
        ct = time.strftime("%Y-%m-%d+%H%M", time.localtime(time.time() - 86400 * num))
        yyyy_mm_dd, hhmm = ct.split("+")[0], "2359"
        mode_class, storer_class = mode_writer_contrast[modename]
        modecreator = mode_class(datatype, yyyy_mm_dd) if mode_class else None
        result = modecreator.dataCollect() if modecreator else None
        # 获取datatype对应入库mongo编号
        m_client = MysqlClient("saas_server")
        mongo_ids = m_client.get_mongoid(datatype)
        m_client.closeMysql()
        for mongo_id in mongo_ids:
            try:
                storer = storer_class(mongo_id)
                storer.write(result, datatype, modename, modecreator, num=num, today=yyyy_mm_dd)
            except:
                import traceback
                print(traceback.print_exc())

if __name__ == "__main__":
    Commands.commonStoreByDaily()
