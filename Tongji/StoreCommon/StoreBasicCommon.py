# -*- coding: utf-8 -*-
import gc
from collections import OrderedDict
from abc import ABCMeta, abstractmethod
from pony.orm import *
from datetime import date
from datetime import timedelta
import time
import datetime
import json

from Tongji.Mode.bindTable import bindTable
from Tongji.AnalysisMap.LogLoder import LogLoder
from SaaSTools.tools import getWeekDays


class StoreBasicCommon(LogLoder):

    def __init__(self, store_freq):
        # 格式：OrderedDict([("name", (analysiser, storer, AnalysisResult, mode))])
        # super(StoreBasicCommon, self).__init__()
        self.storers = OrderedDict([])

    @abstractmethod
    def analysisresult_clear(self):
        pass

    def pipline(self, path, logtype):
        for item in super(StoreBasicCommon, self).pipline(path):
            for line in item:
                try:
                    if logtype == "logfile":
                        yield json.loads(line)
                    elif logtype == "uvfile":
                        yield line
                except:
                    import traceback
                    print(traceback.print_exc())

    def tablename(self, datatype, plat, mainname):
        tablename = "%(datatype)s_%(plat)s_%(mainname)s" % \
                    {"datatype": datatype.lower(), "plat": plat.lower(), "mainname": mainname}
        return tablename

    def dataclear(self, num, table):
        tm = self.tmtype(num)
        delete(item for item in table if item.tm == tm)

    def tmtype(self, num):
        if isinstance(num, int):
            tm = date.today() - timedelta(days=num)
            return tm
        elif isinstance(num, str) or isinstance(num, unicode):
            num = num.replace("-", "").replace(" ", "").replace(":", "").replace("+", "")
            if len(num) == 8:
                tm = datetime.datetime.strptime(num, "%Y%m%d").date()
                return tm
            elif len(num) == 10:
                tm = datetime.datetime.strptime(num, "%Y%m%d%H")
                return tm
            elif len(num) == 12:
                tm = datetime.datetime.strptime(num, "%Y%m%d%H%M").date()
                return tm

    def db_table(self, dbname, datatype, plat, mainname, logtype):
        tablename = self.tablename(datatype, plat, mainname)
        mode = self.storers[(mainname, logtype)][3]
        db = Database()
        table = mode(db)
        db = bindTable(db, table, dbname, tablename)
        return db, table

    def getlogtypes(self):
        result = [storer_key[1] for storer_key in self.storers]
        return list(set(result))

    def print_storers(self):
        print(self.storers)

    def storerate(self, tmsample):
        tmsample = tmsample.replace(" ", "").replace(":", "").replace("+", "").replace("-", "")
        if len(tmsample) == 8:
            return "daily"
        elif len(tmsample) == 10:
            return "hour"
        else:
            return False

    # 根据指定的任务名称执行
    def storeappoint(self, tmsample, dbname, datatype, plat, appoint, ifdel=True, ifclearresult=True):
        if ifclearresult:
            self.analysisresult_clear()
        tmsample = tmsample.replace(" ", "").replace(":", "").replace("+", "").replace("-", "")
        store_rate = self.storerate(tmsample)
        yyyymmdd = tmsample[:8]
        num = (datetime.datetime.today() - datetime.datetime.strptime(yyyymmdd, "%Y%m%d")).days
        if store_rate == "daily":
            hh = "23"
            last = 1440
        elif store_rate == "hour":
            hh = tmsample[8:]
            last = 60
        else:
            return False
        for path in self.paths(num=num, yyyymmdd=yyyymmdd, hhmm=hh+"59", logtype=self.getlogtypes(), datatype=datatype, last=last):
            for data in self.pipline(path.pathfull, logtype=path.pathtype):
                for key in self.storers:
                    if key != (appoint, path.pathtype):
                        continue
                    try:
                        analysiser, storer, result, mode = self.storers[key]
                        # 如果没有 analysiser 一般为一个计算对应多个表的入库。
                        if analysiser is None:
                            continue
                        # 留存、在线天数 这些计算在一次计算中即可得出结果，设置finished为True不会重复执行。
                        if analysiser.finished == True:
                            continue
                        # 执行统计逻辑
                        analysiser.rules(result, data, num=num, dbname=dbname, plat=plat, datatype=datatype, yyyymmdd = yyyymmdd)
                    except:
                        import traceback
                        print(traceback.print_exc())
        for key in self.storers:
            if key[0] != appoint:
                continue
            try:
                analysiser, storer, analysisresult, mode = self.storers[key]
                if analysiser is None:
                    continue
                analysiser.transformresult(analysisresult)
                result = analysisresult.transformresult
                storer(result, num, dbname, datatype, plat, ifdel=ifdel)
            except:
                import traceback
                print(traceback.print_exc())

    # 根据指定的任务名称执行
    def storeappointbydaily(self, num, dbname, datatype, plat, appoint, ifdel=True, ifclearresult=True):
        if ifclearresult:
            self.analysisresult_clear()
        if isinstance(num, int):
            yyyymmdd = time.strftime("%Y%m%d", time.localtime(time.time()-86400*num))
        else:
            yyyymmdd = num.replace(" ", "").replace("-", "")
            if len(yyyymmdd) != 8:
                return False
        for path in self.paths(num=num, yyyymmdd=yyyymmdd, hhmm="2359", logtype=self.getlogtypes(), datatype=datatype, last=1440):
            for data in self.pipline(path.pathfull, logtype=path.pathtype):
                for key in self.storers:
                    if key != (appoint, path.pathtype):
                        # print("WARNING: (%s, %s) is a unknown appoint." % (appoint, path.pathtype))
                        continue
                    try:
                        analysiser, storer, result, mode = self.storers[key]
                        if analysiser is None:
                            continue
                        if analysiser.finished == True:
                            continue
                        analysiser.rules(result, data, num=num, dbname=dbname, plat=plat, datatype=datatype, yyyymmdd = yyyymmdd)
                    except:
                        import traceback
                        print(traceback.print_exc())
        for key in self.storers:
            if key[0] != appoint:
                continue
            try:
                analysiser, storer, analysisresult, mode = self.storers[key]
                if analysiser is None:
                    continue
                analysiser.transformresult(analysisresult)
                result = analysisresult.transformresult
                storer(result, num, dbname, datatype, plat, ifdel=ifdel)
            except:
                import traceback
                print(traceback.print_exc())

    # 根据指定的任务名称执行
    def storeappointbyhour(self, num, dbname, datatype, plat, appoint, ifdel=True, ifclearresult=True):
        if ifclearresult:
            self.analysisresult_clear()
        yyyymmddhh = num.replace(" ", "").replace("-", "").replace(":", "")
        try:
            yyyymmdd = yyyymmddhh[:8]
            hhmm = yyyymmdd[8:10]
        except:
            import traceback
            print(traceback.print_exc(), num, dbname, datatype, plat, appoint)
            return False

        for path in self.paths(num=num, yyyymmdd=yyyymmdd, hhmm=hhmm, logtype=self.getlogtypes(),
                               datatype=datatype, last=60):
            for data in self.pipline(path.pathfull, logtype=path.pathtype):
                for key in self.storers:
                    if key != (appoint, path.pathtype):
                        continue
                    try:
                        analysiser, storer, result, mode = self.storers[key]
                        if analysiser is None:
                            continue
                        if analysiser.finished == True:
                            continue
                        analysiser.rules(result, data, num=num, dbname=dbname, plat=plat, datatype=datatype, yyyymmdd = yyyymmdd)
                    except:
                        import traceback
                        print(traceback.print_exc())
        for key in self.storers:
            if key[0] != appoint:
                continue
            try:
                analysiser, storer, analysisresult, mode = self.storers[key]
                if analysiser is None:
                    continue
                analysiser.transformresult(analysisresult)
                result = analysisresult.transformresult
                storer(result, num, dbname, datatype, plat, ifdel=ifdel)
            except:
                import traceback
                print(traceback.print_exc())



    # 执行所有任务（遍历一遍日志）
    def storeByHour(self, yyyymmddhh, dbname, datatype, plat, ifdel=False, ifclearresult=True):
        if ifclearresult:
            self.analysisresult_clear()
        yyyymmddhh = yyyymmddhh.replace("-", "").replace("+", "").replace(" ", "").replace(":", "")
        yyyymmdd = yyyymmddhh[:8]
        num = int(time.mktime(time.strptime(yyyymmdd, "%Y%m%d")) - time.time()) / 86400
        hhmm = yyyymmddhh[8:] + "59"
        for path in self.paths(num=num, logtype=self.getlogtypes(), yyyymmdd=yyyymmdd, hhmm=hhmm, datatype=datatype,
                               last=60):
            for data in self.pipline(path.pathfull, logtype=path.pathtype):
                for key in self.storers:
                    try:
                        analysis_logtype = key[1]
                        if analysis_logtype != path.pathtype:
                            continue
                        analysiser, storer, result, mode = self.storers[key]
                        if analysiser is None:
                            continue
                        if analysiser.finished == True:
                            continue
                        analysiser.rules(result, data, num=num, dbname=dbname, plat=plat, datatype=datatype, yyyymmdd = yyyymmdd)
                    except:
                        import traceback
                        print(traceback.print_exc())
        for key in self.storers:
            try:
                mainname, logtype = key[0], key[1]
                analysiser, storer, analysisresult, mode = self.storers[key]
                if analysiser is None:
                    continue
                analysiser.transformresult(analysisresult)
                result = analysisresult.transformresult
                storer(result, yyyymmddhh, dbname, datatype, plat, mainname=mainname, logtype=logtype, ifdel=ifdel)
                del result
                analysisresult.cleardata()
                gc.collect()
            except:
                import traceback
                print(traceback.print_exc())

    # 执行所有任务（遍历一遍日志）
    def storeByDaily(self, num, dbname, datatype, plat, ifdel=False, ifclearresult=True, iszip=True):
        if ifclearresult:
            self.analysisresult_clear()
        yyyymmdd = time.strftime("%Y%m%d", time.localtime(time.time()-86400*num))
        for path in self.paths(num=num, logtype=self.getlogtypes(), yyyymmdd=yyyymmdd, hhmm="2359", datatype=datatype, last=1440, iszip=iszip):
            for data in self.pipline(path.pathfull, logtype=path.pathtype):
                for key in self.storers:
                    try:
                        analysis_logtype = key[1]
                        if analysis_logtype != path.pathtype:
                            continue
                        analysiser, storer, result, mode = self.storers[key]
                        if analysiser is None:
                            continue
                        if analysiser.finished == True:
                            continue
                        analysiser.rules(result, data, num=num, dbname=dbname, plat=plat, datatype=datatype, yyyymmdd = yyyymmdd)
                    except:
                        import traceback
                        print(traceback.print_exc())
        for key in self.storers:
            try:
                mainname, logtype = key[0], key[1]
                analysiser, storer, analysisresult, mode = self.storers[key][0], self.storers[key][1], self.storers[key][2], self.storers[key][3]
                if analysiser is None:
                    continue
                analysiser.transformresult(analysisresult)
                result = analysisresult.transformresult
                storer(result, num, dbname, datatype, plat, mainname=mainname, logtype=logtype, ifdel=ifdel)
                del result
                analysisresult.cleardata()
                gc.collect()
            except:
                import traceback
                print(traceback.print_exc())

    # 执行所有任务（遍历一遍日志）
    def storeByWeekThrough(self, num, dbname, datatype, plat, ifdel=False, ifclearresult=True):
        if ifclearresult:
            self.analysisresult_clear()
        week_days = getWeekDays(num, dateformat="%Y%m%d")
        for yyyymmdd in week_days:
        # yyyymmdd = time.strftime("%Y%m%d", time.localtime(time.time()-86400*num))
            for path in self.paths(num=num, logtype=self.getlogtypes(), yyyymmdd=yyyymmdd, hhmm="2359", datatype=datatype, last=1440):
                for data in self.pipline(path.pathfull, logtype=path.pathtype):
                    for key in self.storers:
                        try:
                            analysis_logtype = key[1]
                            if analysis_logtype != path.pathtype:
                                continue
                            analysiser, storer, result, mode = self.storers[key]
                            if analysiser is None:
                                continue
                            if analysiser.finished == True:
                                continue
                            analysiser.rules(result, data, num=num, dbname=dbname, plat=plat, datatype=datatype, yyyymmdd = yyyymmdd)
                        except:
                            import traceback
                            print(traceback.print_exc())
        for key in self.storers:
            try:
                mainname, logtype = key[0], key[1]
                analysiser, storer, analysisresult, mode = self.storers[key][0], self.storers[key][1], self.storers[key][2], self.storers[key][3]
                if analysiser is None:
                    continue
                analysiser.transformresult(analysisresult)
                result = analysisresult.transformresult
                storer(result, num, dbname, datatype, plat, mainname=mainname, logtype=logtype, ifdel=ifdel)
            except:
                import traceback
                print(traceback.print_exc())

    # 执行所有任务（遍历一遍日志）
    def storeByWeek(self, num, dbname, datatype, plat, ifdel=False, ifclearresult=True):
        yyyymmdd = time.strftime("%Y%m%d", time.localtime(time.time()-86400*num))
        # for path in self.paths(num=num, logtype=self.getlogtypes(), yyyymmdd=yyyymmdd, hhmm="2359", datatype=datatype, last=1440):
        #     for data in self.pipline(path.pathfull, logtype=path.pathtype):
        if ifclearresult:
            self.analysisresult_clear()
        data = None
        for key in self.storers:
            try:
                analysiser, storer, result, mode = self.storers[key]
                if analysiser is None:
                    continue
                if analysiser.finished == True:
                    continue
                analysiser.rules(result, data, num=num, dbname=dbname, plat=plat, datatype=datatype, yyyymmdd = yyyymmdd)
            except:
                import traceback
                print(traceback.print_exc())
        for key in self.storers:
            try:
                mainname, logtype = key[0], key[1]
                analysiser, storer, analysisresult, mode = self.storers[key]
                if analysiser is None:
                    continue
                analysiser.transformresult(analysisresult)
                result = analysisresult.transformresult
                storer(result, num, dbname, datatype, plat, mainname=mainname, logtype=logtype, ifdel=ifdel)
            except:
                import traceback
                print(traceback.print_exc())

if __name__ == "__main__":
    tester = StoreBasicCommon()
    print(tester.tablename("datatype", "plat", "mainname"))