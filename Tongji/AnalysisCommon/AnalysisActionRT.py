# -*- coding: utf-8 -*-
from Tongji.AnalysisMap.AnalysisMap import AnalysisMap
import json


class AnalysisActionRT(AnalysisMap):

    def __init__(self):
        self.finished = False

    def rules(self, analysisresult, data, num, *args, **kwargs):
        result = analysisresult.result
        try:
            data = json.loads(data) if not isinstance(data, dict) else data
            uid = data["jhd_userkey"].strip()
            pub = data["jhd_pb"].strip()
            ver = data["jhd_vr"].strip()
            opType = data["jhd_opType"].strip()

            if opType == "action":
                eventid = data["jhd_eventId"].strip()
                # 初始化字典
                result.setdefault((pub, ver, eventid), [set(), 0])
                result.setdefault(("all", ver, eventid), [set(), 0])
                result.setdefault((pub, "all", eventid), [set(), 0])
                result.setdefault(("all", "all", eventid), [set(), 0])
                # 计算用户数（分版本、渠道）
                result[(pub, ver, eventid)][0].add(uid)
                result[(pub, "all", eventid)][0].add(uid)
                result[("all", ver, eventid)][0].add(uid)
                result[("all", "all", eventid)][0].add(uid)
                # 计算次数（分版本、渠道）
                result[(pub, ver, eventid)][1] += 1
                result[("all", ver, eventid)][1] += 1
                result[(pub, "all", eventid)][1] += 1
                result[("all", "all", eventid)][1] += 1
            return True
        except:
            import traceback
            print(traceback.print_exc())
            return False

    def transformresult(self, analysisresult, *args, **kwargs):
        result = analysisresult.result
        analysisresult.transformresult = result

