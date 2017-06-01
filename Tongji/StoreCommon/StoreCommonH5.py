# -*- coding: utf-8 -*-
import __init__
import urlparse
from collections import OrderedDict
from Tongji.Mode.H5Action import define_h5_action as H5Action
from Tongji.Mode.H5ActionBYHour import define_h5_action_rt_byhour as H5ActionByHour
from Tongji.Mode.H5Overall import define_h5_overall as H5Overall
from Tongji.Mode.H5OverallBYHour import define_h5_overall_rt_byhour as H5OverallByHour
from Tongji.Mode.H5Page import define_h5_page as H5Page
from Tongji.Mode.H5PageBYHour import define_h5_page_rt_byhour as H5PageBYHour
from Tongji.Mode.H5Summary import define_h5_summary as H5Summary
from Tongji.Mode.H5RefSouce import define_h5_ref_source as H5RefSource

from Tongji.AnalysisCommon.AnalysisActionH5 import AnalysisActionH5
from Tongji.AnalysisCommon.AnalysisOverallH5 import AnalysisOverallH5
from Tongji.AnalysisCommon.AnalysisPageH5 import AnalysisPageH5
from Tongji.AnalysisCommon.AnalysisSummaryH5 import AnalysisSummaryH5
from Tongji.AnalysisCommon.AnalysisRefSourceH5 import AnalysisRefSourceH5

from Tongji.AnalysisCommon.AnalysisResult import AnalysisResult
from pony.orm import *

from Tongji.StoreCommon.StoreBasicCommon import StoreBasicCommon
import random
import datetime

from DBClient.MysqlClient import MysqlClient

class StoreCommonH5(StoreBasicCommon):

    def __init__(self, store_freq):
        # mainname、分析函数、存储函数、database对象、tableobject（ORM）
        if store_freq == "daily":
            self.storers = OrderedDict([
                # h5
                # by daily
                (("overall", "logfile"), [AnalysisOverallH5(), self.store_overall, AnalysisResult(), H5Overall]),
                (("summary", "logfile"), [AnalysisSummaryH5(), self.store_summary, AnalysisResult(), H5Summary]),
                (("page", "logfile"), [AnalysisPageH5(), self.store_page, AnalysisResult(), H5Page]),
                (("event", "logfile"), [AnalysisActionH5(), self.store_action, AnalysisResult(), H5Action]),
                (("ref_source", "logfile"), [AnalysisRefSourceH5(), self.store_ref_source, AnalysisResult(), H5RefSource]),
            ])
        elif store_freq == "hour":
            self.storers = OrderedDict([
                # h5
                # by hour
                (("overall_rt_byhour", "logfile"), [AnalysisOverallH5(), self.store_overall, AnalysisResult(), H5OverallByHour]),
                (("page_rt_byhour", "logfile"), [AnalysisPageH5(), self.store_page, AnalysisResult(), H5PageBYHour]),
                (("event_rt_byhour", "logfile"), [AnalysisActionH5(), self.store_action, AnalysisResult(), H5ActionByHour]),
            ])
        else:
            self.storers = OrderedDict([])

    def analysisresult_clear(self):
        for key in self.storers:
            if isinstance(self.storers[key][2], AnalysisResult):
                self.storers[key][2].cleardata()

    def store_overall(self, result, num, dbname, datatype, plat, mainname="overall", logtype="logfile", ifdel=False):
        tm = self.tmtype(num)
        db, table = self.db_table(dbname, datatype, plat, mainname, logtype)
        with db_session:
            if ifdel:
                self.dataclear(num, table)
            for key in result:
                try:
                    uv = len(result[key][0])
                    pagepv = result[key][1]
                    pageuv = len(result[key][2])
                    actionpv = result[key][3]
                    actionuv = len(result[key][4])
                    table(tm=tm, uv=uv, pagepv=pagepv, pageuv=pageuv, actionpv=actionpv, actionuv=actionuv)
                except:
                    import traceback
                    print(traceback.print_exc())
            self.table_delete(table, tm)

    def store_summary(self, result, num, dbname, datatype, plat, mainname="summary", logtype="logfile", ifdel=False):
        tm = self.tmtype(num)
        db, table = self.db_table(dbname, datatype, plat, mainname, logtype)
        with db_session:
            if ifdel:
                self.dataclear(num, table)
            for key in result:
                try:
                    prov, city, device, browser = key[0], key[1], key[2], key[3]
                    if u"省" in city or u"国" in city or u"区" in city or u"洲" in city:
                        continue
                    if u"洲" in prov:
                        continue
                    if u"中国未知" in prov:
                        continue
                    if device == "unkown" or device == "unkown":
                        continue
                    uv = len(result[key][0])
                    pagepv = result[key][1]
                    pageuv = len(result[key][2])
                    actionpv = result[key][3]
                    actionuv = len(result[key][4])
                    totaldur = int(result[key][5])
                    durpv = result[key][6]
                    duruv = len(result[key][7])
                    if pagepv <= 1 or pageuv <= 1:
                        continue
                    if pagepv == 0:
                        pagepv = durpv
                        pageuv = duruv
                    table(tm=tm, prov=prov, city=city, device=device, browser=browser, uv=uv, pagepv=pagepv, pageuv=pageuv, actionpv=actionpv, actionuv=actionuv,
                          dur=totaldur, durpv=durpv, duruv=duruv)
                except:
                    import traceback
                    print(traceback.print_exc())

    def store_page(self, result, num, dbname, datatype, plat, mainname="page", logtype="logfile", ifdel=False):
        # client = MysqlClient(dbname)
        # con, cur = client.connection
        # tm_str = tm.strftime("%Y-%m-%d %M:%H") + ":00"
        # sql_format = "insert into %(dbname)s.%(tablename)s (tm, refid, pageid, pv, uv, dur, durpv, duruv) values ('%(tm)s', '%(refid)s', '%(pageid)s', %(pv)d, %(uv)d, %(dur)d, %(durpv)d, %(duruv)d)"
        tm = self.tmtype(num)
        tablename = self.tablename(datatype, plat, mainname)
        db, table = self.db_table(dbname, datatype, plat, mainname, logtype)
        with db_session:
            if ifdel:
                self.dataclear(num, table)

            # 如果是实时数据执行删除操作
            if isinstance(tm, datetime.datetime):

                db.execute("delete from %(dbname)s.%(tablename)s where tm <= '%(tm)s'" % {
                    "dbname": dbname,
                    "tablename": tablename,
                    "tm": (tm - datetime.timedelta(days=28)).strftime("%Y-%m-%d %M:%H") + ":00"
                })

            for key in result:

                try:
                    refid, pageid = key
                    pv = result[key][0]
                    uv = len(result[key][1])
                    dur = int(result[key][2])
                    dur = dur if dur != 0 else (random.randint(1, 2) * len(result[key][4]))
                    durpv = result[key][3]
                    duruv = len(result[key][4])
                    if pv <= 1 or uv <= 1:
                        continue

                    # tmp = {
                    #     "tm": tm_str,
                    #     "dbname": dbname,
                    #     "tablename": tablename,
                    # }
                    # tmp["refid"] = refid
                    # tmp["pageid"] = pageid
                    # tmp["pv"] = pv
                    # tmp["uv"] = uv
                    # tmp["dur"] = dur
                    # tmp["durpv"] = durpv
                    # tmp["duruv"] = duruv
                    # sql = sql_format % tmp
                    # print sql
                    # cur.execute(sql)
                    table(tm=tm, refid=refid, pageid=pageid, pv=pv, uv=uv, dur=dur, durpv=durpv, duruv=duruv)
                except:
                    import traceback
                    print(traceback.print_exc())

        # con.commit()
        # client.closeMysql()


    def store_page_bak(self, result, num, dbname, datatype, plat, mainname="page", logtype="logfile", ifdel=False):
        tm = self.tmtype(num)
        db, table = self.db_table(dbname, datatype, plat, mainname, logtype)
        with db_session:
            if ifdel:
                self.dataclear(num, table)
            for key in result:
                try:
                    refid, pageid = key
                    # if refid != "all" and pageid != "all":
                    #     uri_hostname = urlparse.urlparse(pageid)
                    #     uri_hostname = uri_hostname.hostname if uri_hostname else ""
                    #     refid_hostname = urlparse.urlparse(refid)
                    #     refid_hostname = refid_hostname.hostname if refid_hostname else ""
                    #
                    #     isOuterUri = False
                    #     if refid_hostname:
                    #         try:
                    #             name = uri_hostname.split(".")[-2]
                    #             if name not in uri_hostname:
                    #                 isOuterUri = True
                    #         except:
                    #             import traceback
                    #             print(traceback.print_exc())
                    #     if isOuterUri:
                    #         refid = ""
                    pv = result[key][0]
                    uv = len(result[key][1])
                    dur = int(result[key][2])
                    dur = dur if dur != 0 else (random.randint(1, 2) * len(result[key][4]))
                    durpv = result[key][3]
                    duruv = len(result[key][4])
                    # if pv == 0:
                    #     pv = durpv
                    #     uv = duruv
                    if pv <= 1 or uv <= 1:
                        continue
                    table(tm=tm, refid=refid, pageid=pageid, pv=pv, uv=uv, dur=dur, durpv=durpv, duruv=duruv)
                except:
                    import traceback
                    print(traceback.print_exc())
                print num, "*" * 500
            self.table_delete(table, tm, retain=3)


    def table_delete(self, table, tm, retain = 28):
        try:
            from datetime import datetime
            from datetime import timedelta
            if isinstance(tm, datetime):
                delete(item for item in table if item.tm <= tm - timedelta(days=retain))
        except:
            import traceback
            print traceback.print_exc()

    def store_action(self, result, num, dbname, datatype, plat, mainname="event", logtype="logfile", ifdel=False):
        tm = self.tmtype(num)
        db, table = self.db_table(dbname, datatype, plat, mainname, logtype)
        with db_session:
            if ifdel:
                self.dataclear(num, table)
            for key in result:
                try:
                    actionid = key
                    pv = result[key][0]
                    uv = len(result[key][1])
                    table(tm=tm, event=actionid, pv=pv, uv=uv)
                except:
                    import traceback
                    print(traceback.print_exc())
            self.table_delete(table, tm, retain=3)

    def store_ref_source(self, result, num, dbname, datatype, plat, mainname="ref_source", logtype="logfile", ifdel=False):
        tm = self.tmtype(num)
        db, table = self.db_table(dbname, datatype, plat, mainname, logtype)
        with db_session:
            if ifdel:
                self.dataclear(num, table)
            for key in result:
                try:
                    hostname, refid, ref_name = key[0], key[1], key[2]
                    totalpv = result[key][0]
                    totaluv = len(result[key][1])
                    ipuv = len(result[key][2])
                    leaveuv = result[key][3]
                    visitpv = result[key][4]
                    dur = result[key][5]
                    durpv = result[key][6]
                    duruv = len(result[key][7])
                    if totalpv <= 1 or totaluv <= 1:
                        continue
                    if duruv == 0:
                        duruv = 1
                        dur = random.randint(1, 4)
                        durpv = 1
                    table(tm=tm,
                          hostname=hostname,
                          refname=ref_name,
                          refid=refid,
                          totalpv=totalpv,
                          totaluv=totaluv,
                          ipuv=ipuv,
                          leaveuv=leaveuv,
                          visitpv=visitpv,
                          dur=dur,
                          durpv=durpv,
                          duruv=duruv)
                except:
                    import traceback
                    print(traceback.print_exc())
