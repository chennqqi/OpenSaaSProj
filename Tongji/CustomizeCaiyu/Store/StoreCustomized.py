# -*- coding: utf-8 -*-
import __init__
from collections import OrderedDict
from datetime import date
from datetime import timedelta
from datetime import datetime

from pony.orm import *

from Tongji.CustomizeCaiyu.Mode.PlatPush import define_plat_push as PlatPush
from Tongji.CustomizeCaiyu.Mode.PlatPushDayHour import define_plat_push_day_hour as PlatPushDayHour
from Tongji.CustomizeCaiyu.Mode.PlatPushWeek import define_plat_push_week as PlatPushWeek
from Tongji.CustomizeCaiyu.Analysis import AnalysisPush
from Tongji.CustomizeCaiyu.Analysis import AnalysisPushDayHour
from Tongji.CustomizeCaiyu.Analysis import AnalysisPushWeek
from Tongji.AnalysisCommon.AnalysisResult import AnalysisResult
from Tongji.StoreCustom.StoreBasicCustom import StoreBasicCustom
from SaaSTools.tools import getWeekDays


class StoreCustomized(StoreBasicCustom):

    def __init__(self, store_freq="daily"):
        # 名称、分析函数、存储函数、database对象、tableobject（ORM）
        if store_freq == "daily":
            self.storers = OrderedDict([
                # ("overall", self.store_overall),
                (("push", "logfile"), [AnalysisPush.AnalysisPush(), self.store_push, AnalysisResult(), PlatPush]),
                (("push_day_hour", "logfile"), [AnalysisPushDayHour.AnalysisPushDayHour(), self.store_push_day_hour, AnalysisResult(), PlatPushDayHour]),
            ])
        elif store_freq == "week":
            self.storers = OrderedDict([
                # ("overall", self.store_overall),
                (("push_week", "logfile"), [AnalysisPushWeek.AnalysisPushWeek(), self.store_push_week, AnalysisResult(), PlatPushWeek]),
            ])

    def analysisresult_clear(self):
        for key in self.storers:
            if isinstance(self.storers[key][2], AnalysisResult):
                self.storers[key][2].cleardata()

    def store_push(self, result, num, dbname, datatype, plat, mainname="push", logtype="logfile", ifdel=False):
        tm = date.today() - timedelta(days=num)
        db, table = self.db_table(dbname, datatype, plat, mainname, logtype)
        with db_session:
            if ifdel:
                self.dataclear(num, table)
            for key in result:
                title = key
                try:
                    openpush = result[title][0]
                    awake = result[title][1]
                    cause_ac30 = result[title][2]
                    cause_ac10 = result[title][3]
                    if not title:
                        continue
                    table(tm = tm, title=title, openpush = openpush, awake=awake, cause_ac30 = cause_ac30, cause_ac10 = cause_ac10)
                except:
                    import traceback
                    print(traceback.print_exc())

    def store_push_day_hour(self, result, num, dbname, datatype, plat, mainname="push_day_hour", logtype="logfile", ifdel=False):
        tm = date.today() - timedelta(days=num)
        db, table = self.db_table(dbname, datatype, plat, mainname, logtype)
        with db_session:
            if ifdel:
                self.dataclear(num, table)
            for key in result:
                title, _time_range = key
                try:
                    openpush = result[key][0]
                    awake = result[key][1]
                    cause_ac30 = result[key][2]
                    cause_ac10 = result[key][3]
                    if not title:
                        continue
                    table(tm = tm, hour_range = _time_range, title=title, openpush = openpush, awake=awake, cause_ac30 = cause_ac30, cause_ac10 = cause_ac10)
                except:
                    import traceback
                    print(traceback.print_exc())

    def store_push_week(self, result, num, dbname, datatype, plat, mainname="push_week", logtype="logfile", ifdel=False):
        tm = datetime.strptime(min(getWeekDays(num, dateformat='%Y%m%d').keys()), '%Y%m%d').date()
        db, table = self.db_table(dbname, datatype, plat, mainname, logtype)
        with db_session:
            if ifdel:
                self.dataclear(max(getWeekDays(num, dateformat='%Y%m%d').values()), table)
            for key in result:
                title = key
                try:
                    openpush = result[title][0]
                    awake = result[title][1]
                    cause_ac30 = result[title][2]
                    cause_ac10 = result[title][3]
                    if not title:
                        continue
                    table(tm = tm, title=title, openpush = openpush, awake=awake, cause_ac30 = cause_ac30, cause_ac10 = cause_ac10)
                except:
                    import traceback
                    print(traceback.print_exc())

