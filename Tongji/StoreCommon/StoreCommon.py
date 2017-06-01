# -*- coding: utf-8 -*-
import __init__
from collections import OrderedDict
from SaaSTools.tools import getWeekFirstDay
from SaaSTools.tools import getWeekDays
from Tongji.Mode.PlatEvent import define_plat_event as PlatEvent
from Tongji.Mode.PlatUserConf import define_user_conf as PlatUserConf
from Tongji.Mode.PlatUserConfUdf import define_user_conf_udf as PlatUserConfUdf
from Tongji.Mode.PlatEventBYHour import define_event_rt_byhour as PlatEventBYHour
from Tongji.Mode.PlatInDistribute import define_plat_in_distribute as PlatInDistribute
from Tongji.Mode.PlatUseTimeDistribute import define_plat_usetime_distribute as PlatUseTimeDistribute
from Tongji.Mode.PlatLoc import define_plat_loc as PlatLoc
from Tongji.Mode.PlatMarketOverall import define_plat_market_overall as PlatMarketOverall
from Tongji.Mode.PlatMarketRemain import define_plat_market_remain as PlatMarketRemain
from Tongji.Mode.PlatMarkeyHY import define_plat_market_hy as PlatMarkeyHY
from Tongji.Mode.PlatOS import define_plat_os as PlatOS
from Tongji.Mode.PlatOverall import define_plat_overall as PlatOverall
from Tongji.Mode.PlatOverallBYHour import define_overall_rt_byhour as PlatOverallBYHour
from Tongji.Mode.PlatOverallWeek import define_plat_overall_week as PlatOverallWeek
from Tongji.Mode.PlatPage import define_plat_page as PlatPage
from Tongji.Mode.PlatUA import define_plat_ua as PlatUA

from Tongji.AnalysisCommon.AnalysisAction import AnalysisAction
from Tongji.AnalysisCommon.AnalysisInDistribute import AnalysisInDistribute
from Tongji.AnalysisCommon.AnalysisUseTimeDistribute import AnalysisUseTimeDistribute
from Tongji.AnalysisCommon.AnalysisLoc import AnalysisLoc
from Tongji.AnalysisCommon.AnalysisOnlineDay import AnalysisOnlineDay
from Tongji.AnalysisCommon.AnalysisOverall import AnalysisOverall
from Tongji.AnalysisCommon.AnalysisOverallWeek import AnalysisOverallWeek
from Tongji.AnalysisCommon.AnalysisPage import AnalysisPage
from Tongji.AnalysisCommon.AnalysisRemain import AnalysisRemain
from Tongji.AnalysisCommon.AnalysisUA import AnalysisUA
from Tongji.AnalysisCommon.AnalysisOS import AnalysisOS
from Tongji.AnalysisCommon.AnalysisUserConfUdf import AnalysisUserConfUdf

from Tongji.AnalysisCommon.AnalysisOverallRT import AnalysisOverallRT
from Tongji.AnalysisCommon.AnalysisActionRT import AnalysisActionRT

from Tongji.AnalysisCommon.AnalysisResult import AnalysisResult
from pony.orm import *

from Tongji.StoreCommon.StoreBasicCommon import StoreBasicCommon
from DBClient.MongoData import MongoData
from datetime import date
from datetime import datetime
from datetime import timedelta


class StoreCommon(StoreBasicCommon):

    def __init__(self, store_freq):
        self.store_freq = store_freq
        # 名称、分析函数、存储函数、database对象、tableobject（ORM）
        if store_freq == "hour":
            self.storers = OrderedDict([
                # app
                # by hour
                (("overall_rt_byhour", "logfile"), [AnalysisOverallRT(), self.store_overall_byhour, AnalysisResult(), PlatOverallBYHour]),
                (("event_rt_byhour", "logfile"), [AnalysisActionRT(), self.store_event_byhour, AnalysisResult(), PlatEventBYHour]),
            ])
        elif store_freq == "daily":
            self.storers = OrderedDict([
                # app
                # by daily
                (("overall", "uvfile"), [AnalysisOverall(), self.store_overall, AnalysisResult(), PlatOverall]),
                (("market_overall", "uvfile"), [None, None, None, PlatMarketOverall]),
                (("event", "uvfile"), [AnalysisAction(), self.store_event, AnalysisResult(), PlatEvent]),
                (("user_conf", "uvfile"), [AnalysisAction(), self.store_user_conf, AnalysisResult(), PlatUserConf]),
                (("user_conf_udf", "uvfile"), [AnalysisUserConfUdf(), self.store_user_conf_udf, AnalysisResult(), PlatUserConfUdf]),
                (("page", "uvfile"), [AnalysisPage(), self.store_page, AnalysisResult(), PlatPage]),
                (("in_distribute", "uvfile"), [AnalysisInDistribute(), self.store_in_distribute, AnalysisResult(), PlatInDistribute]),
                (("usetime_distribute", "uvfile"), [AnalysisUseTimeDistribute(), self.store_usetime_distribute, AnalysisResult(), PlatUseTimeDistribute]),
                (("market_hy_7", "uvfile"), [AnalysisOnlineDay(), self.store_hy, AnalysisResult(), PlatMarkeyHY]),
                (("market_remain", "uvfile"), [AnalysisRemain(), self.store_remain, AnalysisResult(), PlatMarketRemain]),
                (("ua", "uvfile"), [AnalysisUA(), self.store_ua, AnalysisResult(), PlatUA]),
                (("os", "uvfile"), [AnalysisOS(), self.store_os, AnalysisResult(), PlatOS]),
                (("loc", "uvfile"), [AnalysisLoc(), self.store_loc, AnalysisResult(), PlatLoc]),
            ])
        elif store_freq == "week":
            self.storers = OrderedDict([
                # app
                # by week
                (("overall_week", "uvfile"), [AnalysisOverallWeek(), self.store_overall_by_week, AnalysisResult(), PlatOverallWeek]),
                (("overall", "uvfile"), [None, None, None, PlatOverall]),
            ])

    def analysisresult_clear(self):
        for key in self.storers:
            if isinstance(self.storers[key][2], AnalysisResult):
                self.storers[key][2].cleardata()
                try:
                    self.storers[key][0].reset()
                except:
                    import traceback
                    print(traceback.print_exc())

    def store_overall(self, result, num, dbname, datatype, plat, mainname="overall", logtype="uvfile", ifdel=False):
        tm = date.today() - timedelta(days=num)
        db, table_overall = self.db_table(dbname, datatype, plat, mainname, logtype)
        with db_session:
            if ifdel:
                self.dataclear(num, table_overall)
            for key in result:
                try:
                    ver, comepub = key[0], key[1]
                    uv, intotal, pagetotal, dur = result[key][0][0], result[key][0][1], result[key][0][2], \
                                                  result[key][0][3]
                    newcomer = result[key][1][0]
                    try:
                        alluser = select(item.alluser for item in table_overall if
                                         item.tm < tm and item.pub == comepub and item.ver == ver).max() + newcomer
                    except:
                        alluser = max([newcomer, uv])
                    table_overall(tm=tm, ver=ver, pub=comepub, newcomer=newcomer, uv=uv, alluser=alluser,
                                   intotal=intotal, pagetotal=pagetotal, durtotal=dur)
                except:
                    print(tm, ver, comepub, newcomer, uv, alluser, intotal, pagetotal, dur)
                    import traceback
                    print(traceback.print_exc())

        db, table_market_overall = self.db_table(dbname, datatype, plat, "market_overall", logtype)
        with db_session:
            if ifdel:
                self.dataclear(num, table_market_overall)
            for key in result:
                try:
                    ver, comepub = key[0], key[1]
                    newcomer = result[key][1][0]
                    intotal, pagetotal, dur = result[key][1][1], result[key][1][2], result[key][1][3]
                    table_market_overall(tm=tm, ver=ver, pub=comepub, newcomer=newcomer,
                                              intotal=intotal, pagetotal=pagetotal, durtotal=dur)
                except:
                    import traceback
                    print(traceback.print_exc())

    def store_overall_byhour(self, result, yyyymmddhh, dbname, datatype, plat, mainname="overall_rt_byhour", logtype="uvfile", ifdel=True):
        tm = datetime.strptime(yyyymmddhh, "%Y%m%d%H")
        yyyymmddhh_his = tm.strftime("%Y%m%d%H")
        if tm.hour != 23:
            tm = tm + timedelta(hours=1)
            yyyymmddhh = tm.strftime("%Y%m%d%H")
        else:
            tm = datetime.strptime(yyyymmddhh + "59", "%Y%m%d%H%M")
            yyyymmddhh = tm.strftime("%Y%m%d%H")
            yyyymmddhhmm = tm.strftime("%Y%m%d%H%M")
        db, table = self.db_table(dbname, datatype, plat, mainname, logtype)
        mongodata = MongoData()
        with db_session:
            if ifdel:
                pass
                # if tm.hour != 23 and tm.minute == 0:
                #     self.dataclear(yyyymmddhh, table)
                # else:
                #     self.dataclear(yyyymmddhhmm, table)
            if result:
                for key in result:
                    try:
                        pub, ver = key[0], key[1]
                        active = len(result[key][0])
                        inpv = result[key][1]
                        pagepv = result[key][2]
                        # newcomer = mongodata.newcomerCount(result[key][0], datatype)
                        newcomer = mongodata.newcomerCount(yyyymmddhh_his, datatype, ver, pub)
                        addnewcomer = mongodata.newcomerAddup(yyyymmddhh_his, datatype, ver, pub)
                        addactive = mongodata.activeAddup(yyyymmddhh_his, datatype, ver, pub)
                        table(tm=tm, pub=pub, ver=ver, active=active, inpv=inpv, pagepv=pagepv, newcomer=newcomer,
                                addactive=addactive, addnewcomer=addnewcomer)
                    except:
                        import traceback
                        print(traceback.print_exc())
            else:
                try:
                    pub, ver = "all", "all"
                    active = 0
                    inpv = 0
                    pagepv = 0
                    newcomer = 0
                    addnewcomer = mongodata.newcomerAddup(yyyymmddhh, datatype, ver, pub)
                    addactive = mongodata.activeAddup(yyyymmddhh, datatype, ver, pub)
                    table(tm=tm, pub=pub, ver=ver, active=active, inpv=inpv, pagepv=pagepv, newcomer=newcomer,
                            addactive=addactive, addnewcomer=addnewcomer)
                    if tm.minute == 59 and tm.hour != 23:
                        table(tm=tm + timedelta(minutes=1),
                              pub=pub,
                              ver=ver,
                              active=0,
                              inpv=0,
                              pagepv=0,
                              newcomer=0,
                              addactive=0,
                              addnewcomer=0)
                except:
                    import traceback
                    print(traceback.print_exc())
            hold_tm = tm - timedelta(days=29)
            delete(item for item in table if item.tm < hold_tm)

    def store_event_byhour(self, result, yyyymmddhh, dbname, datatype, plat, mainname="event_rt_byhour", logtype="uvfile", ifdel=True):
        tm = datetime.strptime(yyyymmddhh, "%Y%m%d%H")
        if tm.hour != 23:
            tm = tm + timedelta(hours=1)
            yyyymmddhh = tm.strftime("%Y%m%d%H")
        else:
            tm = datetime.strptime(yyyymmddhh + "59", "%Y%m%d%H%M")
            yyyymmddhh = tm.strftime("%Y%m%d%H")
            yyyymmddhhmm = tm.strftime("%Y%m%d%H%M")
        db, table = self.db_table(dbname, datatype, plat, mainname, logtype)
        with db_session:
            if ifdel:
                pass
                # if tm.hour != 23 and tm.minute == 0:
                #     self.dataclear(yyyymmddhh, table)
                # else:
                #     self.dataclear(yyyymmddhhmm, table)
            if result:
                for key in result:
                    try:
                        pub, ver, eventid = key[0], key[1], key[2]
                        uv = len(result[key][0])
                        pv = result[key][1]
                        table(tm=tm, pub=pub, ver=ver, eventid=eventid, uv=uv, pv=pv)
                        # if tm.minute == 59:
                        #     table_1(tm=tm + timedelta(minutes=1), pub=pub, ver=ver, eventid=eventid, uv=0, pv=0)
                    except:
                        import traceback
                        print(traceback.print_exc())
            else:
                try:
                    pub, ver, eventid = "all", "all", "all"
                    uv = 0
                    pv = 0
                    # table(tm=tm, pub=pub, ver=ver, eventid=eventid, uv=uv, pv=pv)
                    if tm.minute == 59:
                        table(tm=tm + timedelta(minutes=1), pub=pub, ver=ver, eventid=eventid, uv=0, pv=0)
                except:
                    import traceback
                    print(traceback.print_exc())
            hold_tm = tm - timedelta(days=29)
            delete(item for item in table if item.tm < hold_tm)

    def store_ua(self, result, num, dbname, datatype, plat, mainname="ua", logtype="uvfile", ifdel=False):
        tm = date.today() - timedelta(days=num)
        db, table = self.db_table(dbname, datatype, plat, mainname, logtype)
        with db_session:
            if ifdel:
                self.dataclear(num, table)
            for key in result:
                try:
                    ua, ver, comepub = key[0], key[1], key[2]
                    uv, intotal, pagetotal = result[key][0][0], result[key][0][1], result[key][0][2]
                    newcomer = result[key][1][0]
                    try:
                        alluser = select(item.alluser for item in table if item.tm < tm and item.ua==ua and item.pub==comepub and item.ver==ver).max() + newcomer
                    except:
                        alluser = max([newcomer, uv])
                    table(tm=tm, ua=ua, ver=ver, pub=comepub, \
                                   newcomer=newcomer, uv=uv, alluser=alluser, intotal=intotal, pagetotal=pagetotal)
                except:
                    import traceback
                    print(traceback.print_exc())

    def store_os(self, result, num, dbname, datatype, plat, mainname="os", logtype="uvfile", ifdel=False):
        tm = date.today() - timedelta(days=num)
        db, table = self.db_table(dbname, datatype, plat, mainname, logtype)
        with db_session:
            if ifdel:
                self.dataclear(num, table)
            for key in result:
                try:
                    os, ver, comepub = key[0], key[1], key[2]
                    uv, intotal, pagetotal = result[key][0][0], result[key][0][1], result[key][0][2]
                    newcomer = result[key][1][0]
                    try:
                        alluser = select(item.alluser for item in table if item.tm < tm and item.os==os and item.pub==comepub and item.ver==ver).max() + newcomer
                    except:
                        alluser = max([newcomer, uv])
                    table(tm=tm, os=os, ver=ver, alluser=alluser, pub=comepub, \
                                   newcomer=newcomer, uv=uv, intotal=intotal, pagetotal=pagetotal)
                except:
                    import traceback
                    print(traceback.print_exc())
                    print(key, result[key])


    def store_loc(self, result, num, dbname, datatype, plat, mainname="loc", logtype="uvfile", ifdel=False):
        tm = date.today() - timedelta(days=num)
        db, table = self.db_table(dbname, datatype, plat, mainname, logtype)
        with db_session:
            if ifdel:
                self.dataclear(num, table)
            for key in result:
                try:
                    prov, city, ver, comepub = key[0], key[1], key[2], key[3]
                    prov = prov.decode("utf8")
                    city = city.decode("utf8")
                    if u"中国未知" in prov:
                        continue
                    uv, intotal, pagetotal = result[key][0][0], result[key][0][1], result[key][0][2]
                    newcomer = result[key][1][0]
                    try:
                        alluser = select(item.alluser for item in table if item.tm < tm and item.prov==prov and item.city==city and item.pub==comepub and item.ver==ver).max() + newcomer
                    except:
                        alluser = max([newcomer, uv])
                    table(tm=tm, prov=prov, city=city, alluser=alluser, ver=ver, pub=comepub, \
                                     newcomer=newcomer, uv=uv, intotal=intotal, pagetotal=pagetotal)
                except:
                    import traceback
                    print(traceback.print_exc())

    def store_event(self, result, num, dbname, datatype, plat, mainname="event", logtype="uvfile", ifdel=False):
        tm = date.today() - timedelta(days=num)
        db, table = self.db_table(dbname, datatype, plat, mainname, logtype)
        with db_session:
            if ifdel:
                self.dataclear(num, table)
            for key in result:
                try:
                    action, ver, comepub = key[0], key[1], key[2]
                    uv, pv = result[key][0][0], result[key][0][1]
                    table(tm=tm, event=action, ver=ver, pub=comepub, uv=uv, pv=pv)
                except:
                    import traceback
                    print(traceback.print_exc())

    def store_user_conf(self, result, num, dbname, datatype, plat, mainname="user_conf", logtype="uvfile", ifdel=False):
        tm = date.today() - timedelta(days=num)
        db, table = self.db_table(dbname, datatype, plat, mainname, logtype)

        event_db, event_table = self.db_table(dbname, datatype, plat, "event", "uvfile")

        with db_session:
            if ifdel:
                self.dataclear(num, table)
            for item in event_table.select(lambda c: c.tm == tm and c.ver == "all"  and c.pub == "all"):
                table(tm=item.tm, ver=item.ver, pub=item.pub, nameid=item.event, vshow=str(item.uv), vtype="int")

    def store_user_conf_udf(self, result, num, dbname, datatype, plat, mainname="", logtype="uvfile", ifdel=False):
        # 更新定义表，caiyu_ad_android_eventid_udf
        db_save, table_save = self.db_table(dbname, datatype, plat, "user_conf_udf", "uvfile")
        with db_session:
            for nameid in result:
                name, isdel = result[nameid][0], result[nameid][1]
                conf_obj_uv = table_save.get(nameid=nameid)
                conf_obj_pv = table_save.get(nameid="_".join([nameid, "pv"]))

                if conf_obj_uv or conf_obj_pv:
                    if conf_obj_uv:
                        conf_obj_uv.name = "".join([conf_obj_uv.name, u"" if conf_obj_uv.name.endswith(u"(人数)") else u"(人数)"])
                        conf_obj_uv.enable = 1 if isdel == 0 else 0
                    if conf_obj_pv:
                        conf_obj_pv.name = "".join([conf_obj_pv.name, u"" if conf_obj_pv.name.endswith(u"(次数)") else u"(次数)"])
                        conf_obj_pv.enable = 0
                else:
                    table_save(inserttm=datetime.now(), nameid=nameid,
                               name="".join([name, u"(人数)"]),
                               enable=1 if isdel == 0 else 0
                               )
                    table_save(inserttm=datetime.now(), nameid="_".join([nameid, "pv"]),
                               name="".join([name, u"(次数)"]),
                               enable=0
                               )



    def store_page(self, result, num, dbname, datatype, plat, mainname="page", logtype="uvfile", ifdel=False):
        tm = date.today() - timedelta(days=num)
        db, table = self.db_table(dbname, datatype, plat, mainname, logtype)
        with db_session:
            if ifdel:
                self.dataclear(num, table)
            for key in result:
                try:
                    page, ver, comepub = key[0], key[1], key[2]
                    uv, pv = result[key][0][0], result[key][0][1]
                    table(tm=tm, pageid=page, ver=ver, pub=comepub, uv=uv, pv=pv)
                except:
                    import traceback
                    print(traceback.print_exc())

    def store_in_distribute(self, result, num, dbname, datatype, plat, mainname="in_distribute", logtype="uvfile", ifdel=False):
        tm = date.today() - timedelta(days=num)
        db, table = self.db_table(dbname, datatype, plat, mainname, logtype)
        with db_session:
            if ifdel:
                self.dataclear(num, table)
            for key in result:
                try:
                    ver, comepub = key[0], key[1]
                    uv_active, uv_in, pv_in = result[key][0], result[key][1], result[key][2]
                    uv_1 = result[key][3].get("uv_1", 0)
                    uv_2 = result[key][3].get("uv_2", 0)
                    uv_3 = result[key][3].get("uv_3", 0)
                    uv_4 = result[key][3].get("uv_4", 0)
                    uv_5 = result[key][3].get("uv_5", 0)
                    uv_6 = result[key][3].get("uv_6", 0)
                    uv_7 = result[key][3].get("uv_7", 0)
                    uv_8 = result[key][3].get("uv_8", 0)
                    uv_9 = result[key][3].get("uv_9", 0)
                    uv_10 = result[key][3].get("uv_10", 0)
                    uv_gt10 = result[key][3].get("uv_gt10", 0)
                    table(tm=tm, ver=ver, pub=comepub, uv_active=uv_active, uv_in=uv_in, pv_in=pv_in,
                            uv_1=uv_1, uv_2=uv_2, uv_3=uv_3, uv_4=uv_4, uv_5=uv_5, uv_6=uv_6, uv_7=uv_7, uv_8=uv_8,
                            uv_9=uv_9, uv_10=uv_10, uv_gt10=uv_gt10)
                except:
                    import traceback
                    print(traceback.print_exc())

    def store_usetime_distribute(self, result, num, dbname, datatype, plat, mainname="usetime_distribute", logtype="uvfile", ifdel=False):
        tm = date.today() - timedelta(days=num)
        db, table = self.db_table(dbname, datatype, plat, mainname, logtype)
        with db_session:
            if ifdel:
                self.dataclear(num, table)
            for key in result:
                try:
                    ver, comepub = key[0], key[1]
                    uv_active, uv_dur, pv_dur, dur_total = result[key][0], result[key][1], result[key][2], result[key][4]
                    uv_lte1min = result[key][3].get("uv_lte1min", 0)
                    uv_lte2min = result[key][3].get("uv_lte2min", 0)
                    uv_lte3min = result[key][3].get("uv_lte3min", 0)
                    uv_lte4min = result[key][3].get("uv_lte4min", 0)
                    uv_lte5min = result[key][3].get("uv_lte5min", 0)
                    uv_lte6min = result[key][3].get("uv_lte6min", 0)
                    uv_lte7min = result[key][3].get("uv_lte7min", 0)
                    uv_lte8min = result[key][3].get("uv_lte8min", 0)
                    uv_lte9min = result[key][3].get("uv_lte9min", 0)
                    uv_lte10min = result[key][3].get("uv_lte10min", 0)
                    uv_gt10min = result[key][3].get("uv_gt10min", 0)
                    table(tm=tm, ver=ver, pub=comepub, uv_active=uv_active, uv_dur=uv_dur, pv_dur=pv_dur, dur_total=dur_total,
                            uv_lte1min=uv_lte1min, uv_lte2min=uv_lte2min, uv_lte3min=uv_lte3min, uv_lte4min=uv_lte4min,
                            uv_lte5min=uv_lte5min, uv_lte6min=uv_lte6min, uv_lte7min=uv_lte7min, uv_lte8min=uv_lte8min,
                            uv_lte9min=uv_lte9min, uv_lte10min=uv_lte10min, uv_gt10min=uv_gt10min)
                except:
                    import traceback
                    print(traceback.print_exc())

    def store_remain(self, result, num, dbname, datatype, plat, mainname="market_remain", logtype="uvfile", ifdel=False):
        tm = date.today() - timedelta(days=num)
        db, table = self.db_table(dbname, datatype, plat, mainname, logtype)
        with db_session:
            if ifdel:
                self.dataclear(num, table)
            for key in result:
                items = {}
                ver, comepub = key[0], key[1]
                items.setdefault("ver", ver)
                items.setdefault("pub", comepub)
                for remain in result[key]:
                    try:
                        if remain == 0:
                            table(tm=tm, ver=ver, pub=comepub, remain_0=result[key][remain])
                        elif remain == 1:
                            table.get(tm=tm - timedelta(days=remain), ver=ver,
                                                                    pub=comepub).remain_1 = result[key][remain]
                        elif remain == 3:
                            table.get(tm=tm - timedelta(days=remain), ver=ver,
                                                                    pub=comepub).remain_3 = result[key][remain]
                        elif remain == 7:
                            table.get(tm=tm - timedelta(days=remain), ver=ver,
                                                                    pub=comepub).remain_7 = result[key][remain]
                        elif remain == 15:
                            table.get(tm=tm - timedelta(days=remain), ver=ver,
                                                                    pub=comepub).remain_15 = result[key][remain]
                        elif remain == 30:
                            table.get(tm=tm - timedelta(days=remain), ver=ver,
                                                                    pub=comepub).remain_30 = result[key][remain]
                        elif remain == 90:
                            table.get(tm=tm - timedelta(days=remain), ver=ver,
                                                                    pub=comepub).remain_90 = result[key][remain]
                    except AttributeError:
                        print(("Warning(datatype: %s): switch remain == %d (day: %s, ver:%s, pub: %s) "
                               "object has no attribute") % (datatype, remain, tm.strftime("%Y-%m-%d"), ver, comepub))
                    except:
                        import traceback
                        print(traceback.print_exc())


    def store_hy(self, result, num, dbname, datatype, plat, mainname="market_hy_7", logtype="uvfile", ifdel=False):
        num_last = num + 7
        tm = date.today() - timedelta(days=num_last)
        db, table = self.db_table(dbname, datatype, plat, mainname, logtype)
        hy_days = int(mainname.split("_")[-1])
        assert num_last == hy_days + num, "%s not compare %d" % (mainname, hy_days + num)
        with db_session:
            if ifdel:
                # self.dataclear(1, table)
                # self.dataclear(2, table)
                # self.dataclear(3, table)
                # self.dataclear(4, table)
                # self.dataclear(5, table)
                # self.dataclear(6, table)
                # self.dataclear(7, table)
                self.dataclear(num_last, table)
            for key in result:
                try:
                    items = {}
                    ver, comepub = key[0], key[1]
                    newcomer = result[key][1]
                    for onlinedays in result[key][0]:
                        if onlinedays > hy_days:
                            continue
                        items.setdefault(onlinedays, len(result[key][0][onlinedays]))
                    table(tm=tm, ver=ver, pub=comepub, newcomer=newcomer, \
                                        hy_0=items.get(0, 0), hy_1=items.get(1, 0), hy_2=items.get(2, 0), \
                                        hy_3=items.get(3, 0), hy_4=items.get(4, 0), hy_5=items.get(5, 0), \
                                        hy_6=items.get(6, 0), hy_7=items.get(7, 0))
                except:
                    import traceback
                    print(traceback.print_exc())


    def store_overall_by_week(self, result, num, dbname, datatype, plat, mainname="overall_week", logtype="uvfile", ifdel=False):
        week_first_day_num = getWeekFirstDay(num).values()[0]
        week_days = getWeekDays(num, dateformat="%Y-%m-%d").keys()
        week_days.sort()
        week_end_day = week_days[-1]
        week_end_day = datetime.strptime(week_end_day, "%Y-%m-%d").date()
        tm = date.today() - timedelta(days=week_first_day_num)
        week_first_day = tm
        db_overall_week, table_overall_week = self.db_table(dbname, datatype, plat, mainname, logtype)
        db_overall, table_overall = self.db_table(dbname, datatype, plat, "overall", "uvfile")
        with db_session:
            if ifdel:
                self.dataclear(week_first_day_num, table_overall_week)
            for key in result:
                try:
                    ver, comepub = key[0], key[1]
                    uv = len(result[key])
                    weekData = [(selectitem.uv, selectitem.newcomer, selectitem.alluser) for selectitem in \
                                table_overall.select(lambda item: (item.tm >= week_first_day and item.tm <= week_end_day) and item.pub == comepub and item.ver == ver)]
                    if weekData:
                        sumuv = sum([item[0] for item in weekData])
                        sumnewcomer = sum([item[1] for item in weekData])
                        maxalluser = max([item[2] for item in weekData])
                        assert sumuv >= uv, (ver, comepub, sumuv, uv)
                        maxalluser = uv if uv > maxalluser else maxalluser
                        table_overall_week(tm=tm, ver=ver, pub=comepub, uv=uv, sumuv=sumuv,
                                                          newcomer=sumnewcomer, alluser=maxalluser)
                except:
                    import traceback
                    print(traceback.print_exc())


if __name__ == "__main__":
    tester = StoreCommon("daily")
    # result, num, dbname, datatype, plat, mainname="event", logtype="uvfile", ifdel=False
    # tester.store_user_conf({}, 1, "caiyu", "caiyu_ad", "android", ifdel=True)
    tester.store_event({}, 1, "caiyu", "caiyu_ad", "android", ifdel=True)


