# -*- coding: utf-8 -*-
# from Tongji.AnalysisMap.MapDataFactory import MapDataFactory
import __init__
from Tongji.AnalysisMap.AnalysisMap import AnalysisMap
import datetime
import sys

class AnalysisFlightlineSearchBefore(AnalysisMap):

    def __init__(self):
        super(AnalysisFlightlineSearchBefore, self).__init__()
        # self.mapdata = MapDataFactory().create("log")
        self.deltas = [0, 1, 3, 6, 11, 16, sys.maxint]
        self.part_x_num = len(self.deltas) - 1

    def partition(self, value):
        part_x = [self.deltas.index(part) for part in self.deltas \
                  if (value >= self.deltas[self.deltas.index(part) - 1]) \
                  and (value < self.deltas[self.deltas.index(part)])][0]
        return part_x

    def transformresult(self, analysisresult, *args, **kwargs):
        result = analysisresult.result
        result_transform = {}
        for key in result:
            ver, pub, part_x = key
            pv, uv = result[key][0], len(result[key][1])
            if (ver, pub) not in result_transform:
                uv_all = len(reduce(lambda a, b: a|b, [result.get((ver, pub, i), [0, set(), 0])[1] for i in range(1, self.part_x_num+1)]))
                # pv_all = reduce(lambda a, b: a+b, [result.get((ver, pub, i), [0, set(), 0])[0] for i in range(1, self.part_x_num+1)])
                ahead_total = sum([result.get((ver, pub, i), [0, set(), 0])[2] for i in range(1, self.part_x_num+1)])
                # uv, uv_part, pv_part, sum_aheaddays
                result_transform.setdefault((ver, pub), [uv_all, [0] * self.part_x_num, [0] * self.part_x_num, ahead_total])
            result_transform[(ver, pub)][1][part_x-1] = uv
            result_transform[(ver, pub)][2][part_x-1] = pv
        analysisresult.transformresult = result_transform

    # 用户提前搜索天数
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
            st = mapinfo["st"].replace("-", "") if mapinfo.get("st", None) else None
            # et = mapinfo["et"].replace("-", "") if mapinfo.get("et", None) else None
            opatime = data["jhd_opTime"]
            # opastamp = float(data["jhd_ts"])
            try:
                aheaddays = (datetime.datetime.strptime(st, "%Y%m%d") - \
                    datetime.datetime.strptime(opatime[:8], "%Y%m%d")).days
            except:
                import traceback
                print(traceback.print_exc())
                aheaddays = None

            # 初始化：渠道、版本、区间；次数、人数
            part_x = self.partition(aheaddays)
            result.setdefault((ver, pub, part_x), [0, set(), 0])[0] += 1
            result.setdefault((ver, pub, part_x), [0, set(), 0])[1].add(uid)

            result.setdefault(("all", pub, part_x), [0, set(), 0])[0] += 1
            result.setdefault(("all", pub, part_x), [0, set(), 0])[1].add(uid)

            result.setdefault((ver, "all", part_x), [0, set(), 0])[0] += 1
            result.setdefault((ver, "all", part_x), [0, set(), 0])[1].add(uid)

            result.setdefault(("all", "all", part_x), [0, set(), 0])[0] += 1
            result.setdefault(("all", "all", part_x), [0, set(), 0])[1].add(uid)


            if aheaddays is None:
                aheaddays = 0
            elif aheaddays < 0:
                aheaddays = 0
            elif aheaddays > 60:
                aheaddays = 60
            result.setdefault((ver, pub, part_x), [0, set(), 0])[2] += aheaddays if aheaddays <= 60 else 0
            result.setdefault(("all", pub, part_x), [0, set(), 0])[2] += aheaddays if aheaddays <= 60 else 0
            result.setdefault((ver, "all", part_x), [0, set(), 0])[2] += aheaddays if aheaddays <= 60 else 0
            result.setdefault(("all", "all", part_x), [0, set(), 0])[2] += aheaddays if aheaddays <= 60 else 0
        return result

if __name__ == "__main__":
    tester = AnalysisFlightlineSearchBefore()
    import time
    a = time.time()
    for i in range(0, 20):
        print(i, tester.partition(i))
    print(time.time()-a)
