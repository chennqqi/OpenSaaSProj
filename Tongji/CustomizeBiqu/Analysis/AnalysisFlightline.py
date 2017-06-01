# -*- coding: utf-8 -*-
import __init__
# from Tongji.AnalysisMap.MapDataFactory import MapDataFactory
from Tongji.AnalysisMap.AnalysisMap import AnalysisMap
import datetime
import time


class AnalysisFlightline(AnalysisMap):

    def __init__(self):
        super(AnalysisFlightline, self).__init__()
        # self.mapdata = MapDataFactory().create("log")
        pass

    def transformresult(self, analysisresult, *args, **kwargs):
        analysisresult.transformresult = analysisresult.result

    # 航班信息
    def rules(self, analysisresult, data, num, *args, **kwargs):
        result = analysisresult.result
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
            og = mapinfo["og"]
            dnt = mapinfo["dt"] if mapinfo["dt"] else ""
            st = mapinfo["st"].replace("-", "") if mapinfo.get("st", None) else None
            et = mapinfo["et"].replace("-", "") if mapinfo.get("et", None) else None
            # opatime = data["jhd_opTime"]
            # opastamp = float(data["jhd_ts"])
            # try:
            #     aheaddays = (datetime.datetime.strptime(st, "%Y%m%d") - \
            #         datetime.datetime.strptime(opatime[:8], "%Y%m%d")).days
            #     betweendays = (datetime.datetime.strptime(st, "%Y%m%d") - \
            #         datetime.datetime.strptime(et, "%Y%m%d")).days
            # except:
            #     import traceback
            #     print(traceback.print_exc())

            # 初始化: 单程搜索次数，单程搜索人数，往返搜索次数，往返搜索人数
            result.setdefault((ver, pub, og, dnt), [0, set(), 0, set()])
            result.setdefault((ver, pub, "all", dnt), [0, set(), 0, set()])
            result.setdefault((ver, pub, og, "all"), [0, set(), 0, set()])

            result.setdefault(("all", pub, og, dnt), [0, set(), 0, set()])
            result.setdefault(("all", pub, "all", dnt), [0, set(), 0, set()])
            result.setdefault(("all", pub, og, "all"), [0, set(), 0, set()])

            result.setdefault((ver, "all", og, dnt), [0, set(), 0, set()])
            result.setdefault((ver, "all", "all", dnt), [0, set(), 0, set()])
            result.setdefault((ver, "all", og, "all"), [0, set(), 0, set()])

            result.setdefault(("all", "all", og, dnt), [0, set(), 0, set()])
            result.setdefault(("all", "all", "all", dnt), [0, set(), 0, set()])
            result.setdefault(("all", "all", og, "all"), [0, set(), 0, set()])

            result.setdefault(("all", "all", "all", "all"), [0, set(), 0, set()])

            if og and dnt and (not et):
                result[(ver, pub, og, dnt)][0] += 1
                result[(ver, pub, og, dnt)][1].add(uid)
                result[(ver, pub, "all", dnt)][0] += 1
                result[(ver, pub, "all", dnt)][1].add(uid)
                result[(ver, pub, og, "all")][0] += 1
                result[(ver, pub, og, "all")][1].add(uid)

                result[("all", pub, og, dnt)][0] += 1
                result[("all", pub, og, dnt)][1].add(uid)
                result[("all", pub, "all", dnt)][0] += 1
                result[("all", pub, "all", dnt)][1].add(uid)
                result[("all", pub, og, "all")][0] += 1
                result[("all", pub, og, "all")][1].add(uid)

                result[(ver, "all", og, dnt)][0] += 1
                result[(ver, "all", og, dnt)][1].add(uid)
                result[(ver, "all", "all", dnt)][0] += 1
                result[(ver, "all", "all", dnt)][1].add(uid)
                result[(ver, "all", og, "all")][0] += 1
                result[(ver, "all", og, "all")][1].add(uid)

                result[("all", "all", og, dnt)][0] += 1
                result[("all", "all", og, dnt)][1].add(uid)
                result[("all", "all", "all", dnt)][0] += 1
                result[("all", "all", "all", dnt)][1].add(uid)
                result[("all", "all", og, "all")][0] += 1
                result[("all", "all", og, "all")][1].add(uid)

                result[("all", "all", "all", "all")][0] += 1
                result[("all", "all", "all", "all")][1].add(uid)
            elif og and dnt and et:
                result[(ver, pub, og, dnt)][2] += 1
                result[(ver, pub, og, dnt)][3].add(uid)
                result[(ver, pub, "all", dnt)][2] += 1
                result[(ver, pub, "all", dnt)][3].add(uid)
                result[(ver, pub, og, "all")][2] += 1
                result[(ver, pub, og, "all")][3].add(uid)

                result[("all", pub, og, dnt)][2] += 1
                result[("all", pub, og, dnt)][3].add(uid)
                result[("all", pub, "all", dnt)][2] += 1
                result[("all", pub, "all", dnt)][3].add(uid)
                result[("all", pub, og, "all")][2] += 1
                result[("all", pub, og, "all")][3].add(uid)

                result[(ver, "all", og, dnt)][2] += 1
                result[(ver, "all", og, dnt)][3].add(uid)
                result[(ver, "all", "all", dnt)][2] += 1
                result[(ver, "all", "all", dnt)][3].add(uid)
                result[(ver, "all", og, "all")][2] += 1
                result[(ver, "all", og, "all")][3].add(uid)

                result[("all", "all", og, dnt)][2] += 1
                result[("all", "all", og, dnt)][3].add(uid)
                result[("all", "all", "all", dnt)][2] += 1
                result[("all", "all", "all", dnt)][3].add(uid)
                result[("all", "all", og, "all")][2] += 1
                result[("all", "all", og, "all")][3].add(uid)

                result[("all", "all", "all", "all")][2] += 1
                result[("all", "all", "all", "all")][3].add(uid)
        return result


