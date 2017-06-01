# -*- coding: utf-8 -*-
import __init__
from collections import OrderedDict
from Tongji.CustomizeBiqu.Mode.PlatFlightline import define_flightline as PlatFlightline
from Tongji.CustomizeBiqu.Mode.PlatFlightlineArrival import define_flightline_arrival as PlatFlightlineArrival
from Tongji.CustomizeBiqu.Mode.PlatFlightlineSearchBefore import define_flightline_search_before as PlatFlightlineSearchBefore
from Tongji.CustomizeBiqu.Mode.PlatFlightlineSearchBetween import define_flightline_search_between as PlatFlightlineSearchBetween
from Tongji.CustomizeBiqu.Mode.PlatSearchSubscribe import define_search_subscribe as PlatSearchSubscribe

from Tongji.CustomizeBiqu.Analysis import AnalysisFlightline
from Tongji.CustomizeBiqu.Analysis import AnalysisFlightlineArrival
from Tongji.CustomizeBiqu.Analysis import AnalysisFlightlineSearchBefore
from Tongji.CustomizeBiqu.Analysis import AnalysisFlightlineSearchBetween
from Tongji.CustomizeBiqu.Analysis import AnalysisSearchSubscribe

from Tongji.AnalysisCommon.AnalysisResult import AnalysisResult
from pony.orm import *

from Tongji.StoreCustom.StoreBasicCustom import StoreBasicCustom
from datetime import date
from datetime import timedelta


class StoreCustomized(StoreBasicCustom):

    def __init__(self, store_freq = "daily"):
        # 名称、分析函数、存储函数、database对象、tableobject（ORM）
        if store_freq == "daily":
            self.storers = OrderedDict([
                # ("overall", self.store_overall),
                (("custom_hx", "logfile"), [AnalysisFlightline.AnalysisFlightline(), self.store_flightline, AnalysisResult(), PlatFlightline]),
                (("custom_hx_arrival", "logfile"), [AnalysisFlightlineArrival.AnalysisFlightlineArrival(), self.store_flightline_arrival, AnalysisResult(), PlatFlightlineArrival]),
                (("custom_hx_searchbefore", "logfile"), [AnalysisFlightlineSearchBefore.AnalysisFlightlineSearchBefore(), self.store_flightline_search_before, AnalysisResult(), PlatFlightlineSearchBefore]),
                (("custom_hx_searchbetween", "logfile"), [AnalysisFlightlineSearchBetween.AnalysisFlightlineSearchBetween(), self.store_flightline_search_between, AnalysisResult(), PlatFlightlineSearchBetween]),
                (("custom_hx_searchsub", "logfile"), [AnalysisSearchSubscribe.AnalysisSearchSubscribe(), self.store_flightline_search_subscribe, AnalysisResult(), PlatSearchSubscribe]),
            ])
        else:
            self.storers = OrderedDict([])

    def analysisresult_clear(self):
        for key in self.storers:
            if isinstance(self.storers[key][2], AnalysisResult):
                self.storers[key][2].cleardata()

    # @fn_timer
    def store_flightline(self, result, num, dbname, datatype, plat, mainname="custom_hx", logtype="logfile", ifdel=False):
        tm = date.today() - timedelta(days=num)
        db, table = self.db_table(dbname, datatype, plat, mainname, logtype)
        with db_session:
            if ifdel:
                self.dataclear(num, table)
            for key in result:
                try:
                    ver, pub, og, dest = key[0], key[1], key[2], key[3]
                    singlepv = result[key][0]
                    singleuv = len(result[key][1])
                    roundpv = result[key][2]
                    rounduv = len(result[key][3])
                    table(tm = tm, pub = pub, ver = ver, og = og, dest = dest,
                          singleuv = singleuv, singlepv = singlepv,
                          rounduv = rounduv, roundpv = roundpv)
                except:
                    import traceback
                    print(traceback.print_exc())

    # @fn_timer
    def store_flightline_arrival(self, result, num, dbname, datatype, plat, mainname="custom_hx_arrival", logtype="logfile", ifdel=False):
        tm = date.today() - timedelta(days=num)
        db, table = self.db_table(dbname, datatype, plat, mainname, logtype)
        with db_session:
            if ifdel:
                self.dataclear(num, table)
            for key in result:
                try:
                    ver, pub = key[0], key[1]
                    uv_all = result[key][0]
                    hour_0 = result[key][1][0]
                    hour_1 = result[key][1][1]
                    hour_2 = result[key][1][2]
                    hour_3 = result[key][1][3]
                    hour_4 = result[key][1][4]
                    hour_5 = result[key][1][5]
                    hour_6 = result[key][1][6]
                    hour_7 = result[key][1][7]
                    hour_8 = result[key][1][8]
                    hour_9 = result[key][1][9]
                    hour_10 = result[key][1][10]
                    hour_11 = result[key][1][11]
                    hour_12 = result[key][1][12]
                    hour_13 = result[key][1][13]
                    hour_14 = result[key][1][14]
                    hour_15 = result[key][1][15]
                    hour_16 = result[key][1][16]
                    hour_17 = result[key][1][17]
                    hour_18 = result[key][1][18]
                    hour_19 = result[key][1][19]
                    hour_20 = result[key][1][20]
                    hour_21 = result[key][1][21]
                    hour_22 = result[key][1][22]
                    hour_23 = result[key][1][23]

                    pv_all = sum(result[key][2])
                    hourpv_0 = result[key][2][0]
                    hourpv_1 = result[key][2][1]
                    hourpv_2 = result[key][2][2]
                    hourpv_3 = result[key][2][3]
                    hourpv_4 = result[key][2][4]
                    hourpv_5 = result[key][2][5]
                    hourpv_6 = result[key][2][6]
                    hourpv_7 = result[key][2][7]
                    hourpv_8 = result[key][2][8]
                    hourpv_9 = result[key][2][9]
                    hourpv_10 = result[key][2][10]
                    hourpv_11 = result[key][2][11]
                    hourpv_12 = result[key][2][12]
                    hourpv_13 = result[key][2][13]
                    hourpv_14 = result[key][2][14]
                    hourpv_15 = result[key][2][15]
                    hourpv_16 = result[key][2][16]
                    hourpv_17 = result[key][2][17]
                    hourpv_18 = result[key][2][18]
                    hourpv_19 = result[key][2][19]
                    hourpv_20 = result[key][2][20]
                    hourpv_21 = result[key][2][21]
                    hourpv_22 = result[key][2][22]
                    hourpv_23 = result[key][2][23]
                    table(tm = tm, pub = pub, ver = ver, uv_all=uv_all, pv_all=pv_all,
                          hour_0=hour_0,
                          hour_1=hour_1,
                          hour_2=hour_2,
                          hour_3=hour_3,
                          hour_4=hour_4,
                          hour_5=hour_5,
                          hour_6=hour_6,
                          hour_7=hour_7,
                          hour_8=hour_8,
                          hour_9=hour_9,
                          hour_10=hour_10,
                          hour_11=hour_11,
                          hour_12=hour_12,
                          hour_13=hour_13,
                          hour_14=hour_14,
                          hour_15=hour_15,
                          hour_16=hour_16,
                          hour_17=hour_17,
                          hour_18=hour_18,
                          hour_19=hour_19,
                          hour_20=hour_20,
                          hour_21=hour_21,
                          hour_22=hour_22,
                          hour_23=hour_23,

                          hourpv_0=hourpv_0,
                          hourpv_1=hourpv_1,
                          hourpv_2=hourpv_2,
                          hourpv_3=hourpv_3,
                          hourpv_4=hourpv_4,
                          hourpv_5=hourpv_5,
                          hourpv_6=hourpv_6,
                          hourpv_7=hourpv_7,
                          hourpv_8=hourpv_8,
                          hourpv_9=hourpv_9,
                          hourpv_10=hourpv_10,
                          hourpv_11=hourpv_11,
                          hourpv_12=hourpv_12,
                          hourpv_13=hourpv_13,
                          hourpv_14=hourpv_14,
                          hourpv_15=hourpv_15,
                          hourpv_16=hourpv_16,
                          hourpv_17=hourpv_17,
                          hourpv_18=hourpv_18,
                          hourpv_19=hourpv_19,
                          hourpv_20=hourpv_20,
                          hourpv_21=hourpv_21,
                          hourpv_22=hourpv_22,
                          hourpv_23=hourpv_23,
                          )
                except:
                    import traceback
                    print(traceback.print_exc())

    # @fn_timer
    def store_flightline_search_before(self, result, num, dbname, datatype, plat, mainname="custom_hx_searchbefore", logtype="logfile", ifdel=False):
        tm = date.today() - timedelta(days=num)
        db, table = self.db_table(dbname, datatype, plat, mainname, logtype)
        with db_session:
            if ifdel:
                self.dataclear(num, table)
            for key in result:
                try:
                    ver, pub = key[0], key[1]
                    uv_all = result[key][0]
                    uv_1 = result[key][1][0]
                    uv_2 = result[key][1][1]
                    uv_3 = result[key][1][2]
                    uv_4 = result[key][1][3]
                    uv_5 = result[key][1][4]
                    uv_6 = result[key][1][5]
                    ahead_total = result[key][3]
                    pv_all = sum(result[key][2])
                    pv_1 = result[key][2][0]
                    pv_2 = result[key][2][1]
                    pv_3 = result[key][2][2]
                    pv_4 = result[key][2][3]
                    pv_5 = result[key][2][4]
                    pv_6 = result[key][2][5]
                    table(tm=tm, pub=pub, ver = ver, uv_all=uv_all, pv_all=pv_all, ahead_total=ahead_total, uv_1=uv_1, uv_2=uv_2, uv_3=uv_3, uv_4=uv_4, uv_5=uv_5, uv_6=uv_6,\
                          pv_1=pv_1, pv_2=pv_2, pv_3=pv_3, pv_4=pv_4, pv_5=pv_5, pv_6=pv_6)
                except:
                    import traceback
                    print(traceback.print_exc())

    # @fn_timer
    def store_flightline_search_between(self, result, num, dbname, datatype, plat, mainname="custom_hx_searchbetween", logtype="logfile", ifdel=False):
        tm = date.today() - timedelta(days=num)
        db, table = self.db_table(dbname, datatype, plat, mainname, logtype)
        with db_session:
            if ifdel:
                self.dataclear(num, table)
            for key in result:
                try:
                    ver, pub = key[0], key[1]
                    uv_all = result[key][0]
                    uv_1 = result[key][1][0]
                    uv_2 = result[key][1][1]
                    uv_3 = result[key][1][2]
                    uv_4 = result[key][1][3]
                    uv_5 = result[key][1][4]
                    uv_6 = result[key][1][5]
                    between_total = result[key][3]
                    pv_all = sum(result[key][2])
                    pv_1 = result[key][2][0]
                    pv_2 = result[key][2][1]
                    pv_3 = result[key][2][2]
                    pv_4 = result[key][2][3]
                    pv_5 = result[key][2][4]
                    pv_6 = result[key][2][5]
                    table(tm=tm, pub=pub, ver=ver, uv_all=uv_all, pv_all=pv_all, between_total=between_total, uv_1=uv_1, uv_2=uv_2, uv_3=uv_3, uv_4=uv_4, uv_5=uv_5, uv_6=uv_6, \
                          pv_1=pv_1, pv_2=pv_2, pv_3=pv_3, pv_4=pv_4, pv_5=pv_5, pv_6=pv_6)
                except:
                    import traceback
                    print(traceback.print_exc())

    # @fn_timer
    def store_flightline_search_subscribe(self, result, num, dbname, datatype, plat, mainname="custom_hx_searchsub", logtype="logfile", ifdel=False):
        tm = date.today() - timedelta(days=num)
        db, table = self.db_table(dbname, datatype, plat, mainname, logtype)
        with db_session:
            if ifdel:
                self.dataclear(num, table)
            for key in result:
                try:
                    ver, pub = key[0], key[1]
                    searchall = result[key][0]
                    searchsingle = result[key][1]
                    searchround = result[key][2]
                    bookall = result[key][3]
                    booksingle = result[key][4]
                    bookround = result[key][5]
                    searchsingle_booksingle = result[key][6]
                    searchsingle_bookround = result[key][7]
                    searchround_booksingle = result[key][8]
                    searchround_bookround = result[key][9]
                    table(tm=tm, pub=pub, ver=ver, searchall=searchall, searchsingle=searchsingle, searchround=searchround,
                          bookall=bookall, booksingle=booksingle, bookround=bookround,
                          searchsingle_booksingle=searchsingle_booksingle,
                          searchsingle_bookround=searchsingle_bookround,
                          searchround_booksingle=searchround_booksingle,
                          searchround_bookround=searchround_bookround)
                except:
                    import traceback
                    print(traceback.print_exc())

        # @fn_timer
        def store_flightline_overall(self, result, num, dbname, datatype, plat, mainname="custom_overall",
                                              logtype="uvfile", ifdel=False):
            tm = date.today() - timedelta(days=num)
            db, table = self.db_table(dbname, datatype, plat, mainname, logtype)
            with db_session:
                if ifdel:
                    self.dataclear(num, table)
                for key in result:
                    try:
                        ver, pub = key[0], key[1]
                        searchall = result[key][0]
                        searchsingle = result[key][1]
                        searchround = result[key][2]
                        bookall = result[key][3]
                        booksingle = result[key][4]
                        bookround = result[key][5]
                        searchsingle_booksingle = result[key][6]
                        searchsingle_bookround = result[key][7]
                        searchround_booksingle = result[key][8]
                        searchround_bookround = result[key][9]
                        table(tm=tm, pub=pub, ver=ver, searchall=searchall, searchsingle=searchsingle,
                              searchround=searchround,
                              bookall=bookall, booksingle=booksingle, bookround=bookround,
                              searchsingle_booksingle=searchsingle_booksingle,
                              searchsingle_bookround=searchsingle_bookround,
                              searchround_booksingle=searchround_booksingle,
                              searchround_bookround=searchround_bookround)
                    except:
                        import traceback
                        print(traceback.print_exc())

if __name__ == "__main__":
    tester = StoreCustomized()
    print(tester.print_storers())

