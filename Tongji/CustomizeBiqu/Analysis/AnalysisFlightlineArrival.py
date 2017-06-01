# -*- coding: utf-8 -*-
import __init__
# from Tongji.AnalysisMap.MapDataFactory import MapDataFactory
from Tongji.AnalysisMap.AnalysisMap import AnalysisMap

class AnalysisFlightlineArrival(AnalysisMap):

    def __init__(self):
        super(AnalysisFlightlineArrival, self).__init__()
        pass

    def transformresult(self, analysisresult, *args, **kwargs):
        result = analysisresult.result
        result_transform = {}
        for key in result:
            ver, pub, hour = key
            pv, uv = result[key][0], len(result[key][1])
            if (ver, pub) not in result_transform:
                users = reduce(lambda a, b: a|b, [result.get((ver, pub, i), [0, set()])[1] for i in range(0, 24)])
                uv_all = len(users)
                result_transform.setdefault((ver, pub), [uv_all, [0] * 24, [0] * 24])
            result_transform[(ver, pub)][1][hour] = uv
            result_transform[(ver, pub)][2][hour] = pv
        analysisresult.transformresult = result_transform

    def rules(self, analysisresult, data, num, *args, **kwargs):
        result = analysisresult.result
        eventid = data["jhd_eventId"]
        if eventid == "ac49":
            uid = data["jhd_userkey"]
            mapinfo = data["jhd_map"]
            if not mapinfo:
                return result
            ver = data["jhd_vr"]
            pub = data["jhd_pb"]
            # og 始发地
            # dt 目的地
            # st 去程时间
            # et 返程时间
            # og = mapinfo["og"]
            # dest = mapinfo["og"]
            # st = mapinfo["st"].replace("-", "").replace(",", "").replace(":", "").replace(" ", "") if mapinfo.get("st", None) else None
            at = mapinfo["at"].replace("-", "").replace(",", "").replace(":", "").replace(" ", "") if mapinfo.get("at", None) else None
            hour = int(at[8:10])
            # 初始化：
            result.setdefault((ver, pub, hour), [0, set()])
            result.setdefault((ver, "all", hour), [0, set()])
            result.setdefault(("all", pub, hour), [0, set()])
            result.setdefault(("all", "all", hour), [0, set()])
            # 多
            if hour >= 0 and hour <= 23:
                result[(ver, pub, hour)][0] += 1
                result[(ver, pub, hour)][1].add(uid)
                result[(ver, "all", hour)][0] += 1
                result[(ver, "all", hour)][1].add(uid)
                result[("all", pub, hour)][0] += 1
                result[("all", pub, hour)][1].add(uid)
                result[("all", "all", hour)][0] += 1
                result[("all", "all", hour)][1].add(uid)
            else:
                import random
                hour = random.randint(0, 23)
                result[(ver, pub, hour)][0] += 1
                result[(ver, pub, hour)][1].add(uid)
                result[(ver, "all", hour)][0] += 1
                result[(ver, "all", hour)][1].add(uid)
                result[("all", pub, hour)][0] += 1
                result[("all", pub, hour)][1].add(uid)
                result[("all", "all", hour)][0] += 1
                result[("all", "all", hour)][1].add(uid)
        return result
