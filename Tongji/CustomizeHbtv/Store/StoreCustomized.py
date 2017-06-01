# -*- coding: utf-8 -*-
import __init__
from collections import OrderedDict
from Tongji.CustomizeHbtv.Mode.PlatOverall import define_plat_overall as PlatOverall
from Tongji.CustomizeHbtv.Mode.PlatPlayRank import define_plat_play_rank as PlatPlayRank
from Tongji.CustomizeHbtv.Mode.PlatCourseRank import define_plat_course_rank as PlatCourseRank


from Tongji.CustomizeHbtv.Analysis import AnalysisOverall
from Tongji.CustomizeHbtv.Analysis import AnalysisPlayRank
from Tongji.CustomizeHbtv.Analysis import AnalysisCourseRank

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
                (("overall_custom", "uvfile"), [AnalysisOverall.AnalysisOverall(), self.store_overall, AnalysisResult(), PlatOverall]),
                (("play_rank_custom", "logfile"), [AnalysisPlayRank.AnalysisPlayRank(), self.store_play_rank, AnalysisResult(), PlatPlayRank]),
                (("course_rank_custom", "logfile"), [AnalysisCourseRank.AnalysisCourseRank(), self.store_course_rank, AnalysisResult(), PlatCourseRank]),
            ])
        else:
            self.storers = OrderedDict([])

    def analysisresult_clear(self):
        for key in self.storers:
            if isinstance(self.storers[key][2], AnalysisResult):
                self.storers[key][2].cleardata()

    # @fn_timer
    def store_overall(self, result, num, dbname, datatype, plat, mainname="overall_custom", logtype="uvfile", ifdel=False):
        tm = date.today() - timedelta(days=num)
        db, table = self.db_table(dbname, datatype, plat, mainname, logtype)
        with db_session:
            if ifdel:
                self.dataclear(num, table)
            for key in result:
                try:
                    ver, comepub = key[0], key[1]
                    uv, intotal, pagetotal, dur, playpv, playuv, courseuv = result[key][0][0], result[key][0][1], result[key][0][2], \
                                                  result[key][0][3], result[key][0][6], result[key][0][5], result[key][0][4]
                    newcomer = result[key][1][0]
                    try:
                        alluser = max(item.alluser for item in table if
                                      item.pub == comepub and item.ver == ver and item.tm < tm) + newcomer
                    except:
                        alluser = max([newcomer, uv])
                    table(tm=tm, ver=ver, pub=comepub, newcomer=newcomer, uv=uv, playuv=playuv, playpv=playpv, courseuv=courseuv,alluser=alluser,
                                             intotal=intotal, pagetotal=pagetotal, durtotal=dur)
                except:
                    import traceback
                    print(traceback.print_exc())
        db.disconnect()

    def store_play_rank(self, result, num, dbname, datatype, plat, mainname="play_rank_custom", logtype="uvfile", ifdel=False):
        tm = date.today() - timedelta(days=num)
        db, table = self.db_table(dbname, datatype, plat, mainname, logtype)
        with db_session:
            if ifdel:
                self.dataclear(num, table)
            for key in result:
                try:
                    title = key
                    uv = len(result[key][0])
                    pv = result[key][1]
                    playprogress = sum(result[key][2])/len(result[key][2])
                    table(tm=tm, title=title, uv=uv, pv=pv, finishper=playprogress)
                except:
                    import traceback
                    print(traceback.print_exc())
        db.disconnect()

    def store_course_rank(self, result, num, dbname, datatype, plat, mainname="course_rank_custom", logtype="uvfile", ifdel=False):
        tm = date.today() - timedelta(days=num)
        db, table = self.db_table(dbname, datatype, plat, mainname, logtype)
        with db_session:
            if ifdel:
                self.dataclear(num, table)
            for key in result:
                try:
                    title = key
                    uv, pv = len(result[key][0]), result[key][1]
                    ynumper = sum(result[key][2]) / uv
                    table(tm=tm, title=title, uv=uv, pv=pv, ynumav=ynumper)
                except:
                    import traceback
                    print(traceback.print_exc())
        db.disconnect()

if __name__ == "__main__":
    tester = StoreCustomized()
    print(tester.print_storers())

