# -*- coding: utf-8 -*-
# from Tongji.AnalysisMap.MapDataFactory import MapDataFactory
import __init__
from Tongji.AnalysisMap.AnalysisMap import AnalysisMap
import datetime
import sys
from Tongji.AnalysisCommon.IsNull import IsNull

class AnalysisFlightlineSearchBetween(AnalysisMap):

    def __init__(self):
        super(AnalysisFlightlineSearchBetween, self).__init__()
        # self.mapdata = MapDataFactory().create("log")
        self.deltas = [0, 1, 2, 3, 6, 10, sys.maxint]
        self.betweendaylist = [0]
        self.part_x_num = len(self.deltas) - 1

    def partition(self, value):
        # print(value, '-'*200)
        part_x = [self.deltas.index(part) for part in self.deltas \
                  if (value >= self.deltas[self.deltas.index(part) - 1]) \
                  and (value < self.deltas[self.deltas.index(part)])][0]
        return part_x

    def betweendaysavg(self):
        return sum(self.betweendaylist)/len(self.betweendaylist)

    def transformresult(self, analysisresult, *args, **kwargs):
        result = analysisresult.result
        result_transform = {}
        for key in result:
            ver, pub, part_x = key[0], key[1], key[2]
            pv, uv = result[key][0], len(result[key][1])
            if (ver, pub) not in result_transform:
                uv_all = len(reduce(lambda a, b: a|b, [result.get((ver, pub, i), [0, set(), 0])[1] for i in range(1, self.part_x_num+1)]))
                between_total = sum([result.get((ver, pub, i), [0, set(), 0])[2] for i in range(1, self.part_x_num+1)])
                # uv, uv_part, pv_part, sum_betweendays
                result_transform.setdefault((ver, pub), [uv_all, [0] * self.part_x_num, [0] * self.part_x_num, between_total])
            result_transform[(ver, pub)][1][part_x-1] = uv
            result_transform[(ver, pub)][2][part_x-1] = pv
        analysisresult.transformresult = result_transform

    # 用户搜索航班间隔天数
    def rules(self, analysisresult, data, num, *args, **kwargs):
        result = analysisresult.result
        # deltas = [1, 3, 6, 11, 16, sys.maxint]
        eventid = data["jhd_eventId"]
        if eventid != "ac36":
            return result
        else:
            uid = data["jhd_userkey"]
            mapinfo = data["jhd_map"]
            ver = data["jhd_vr"]
            pub = data["jhd_pb"]
            if not mapinfo:
                return result
            # og 始发地
            # dt 目的地
            # st 去程时间
            # et 返程时间
            # og = mapinfo["og"]
            # dnt = mapinfo["og"]
            #
            # opatime = data["jhd_opTime"]
            # opastamp = float(data["jhd_ts"])
            try:
                st = mapinfo["st"].replace("-", "") if not IsNull.judge(mapinfo.get("st", None)) else None
                et = mapinfo["et"].replace("-", "") if not IsNull.judge(mapinfo.get("et", None)) else None
                if not et:
                    return result
                betweendays = (datetime.datetime.strptime(et, "%Y%m%d") - \
                    datetime.datetime.strptime(st, "%Y%m%d")).days
            except:
                # print(mapinfo)
                # import traceback
                # print(traceback.print_exc())
                betweendays = None
            if betweendays is None:
                betweendays = -1
            elif betweendays < 0:
                betweendays = 0
            elif betweendays > 60:
                betweendays = 60
            # 初始化：渠道、版本、区间；次数、人数
            # print(st, et, betweendays)
            self.betweendaylist.append(betweendays) if (betweendays >= 0 and betweendays <= 60) else None
            betweendays = betweendays if (betweendays >= 0 and betweendays <= 60) else self.betweendaysavg()
            part_x = self.partition(betweendays)
            result.setdefault((ver, pub, part_x), [0, set(), 0])[0] += 1
            result.setdefault((ver, pub, part_x), [0, set(), 0])[1].add(uid)
            result.setdefault((ver, pub, part_x), [0, set(), 0])[2] += betweendays if (betweendays >= 0 and betweendays <= 60) else self.betweendaysavg()
            result.setdefault(("all", pub, part_x), [0, set(), 0])[0] += 1
            result.setdefault(("all", pub, part_x), [0, set(), 0])[1].add(uid)
            result.setdefault(("all", pub, part_x), [0, set(), 0])[2] += betweendays if (betweendays >= 0 and betweendays <= 60) else self.betweendaysavg()
            result.setdefault((ver, "all", part_x), [0, set(), 0])[0] += 1
            result.setdefault((ver, "all", part_x), [0, set(), 0])[1].add(uid)
            result.setdefault((ver, "all", part_x), [0, set(), 0])[2] += betweendays if (betweendays >= 0 and betweendays <= 60) else self.betweendaysavg()
            result.setdefault(("all", "all", part_x), [0, set(), 0])[0] += 1
            result.setdefault(("all", "all", part_x), [0, set(), 0])[1].add(uid)
            result.setdefault(("all", "all", part_x), [0, set(), 0])[2] += betweendays if (betweendays >= 0 and betweendays <= 60) else self.betweendaysavg()

        return result