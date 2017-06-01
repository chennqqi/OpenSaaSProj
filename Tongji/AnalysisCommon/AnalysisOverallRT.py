# -*- coding: utf-8 -*-
from Tongji.AnalysisMap.AnalysisMap import AnalysisMap
import json
import time

class AnalysisOverallRT(AnalysisMap):

    def __init__(self):
        self.finished = False


    def rules(self, analysisresult, data, num, *args, **kwargs):
        result = analysisresult.result
        # curDay = time.strftime("%Y%m%d", time.localtime(time.time() - 86400 * num))
        datatype = kwargs.get("datatype", None)
        try:
            data = json.loads(data) if not isinstance(data, dict) else data
            uid = data["jhd_userkey"].strip()
            pub = data["jhd_pb"].strip()
            ver = data["jhd_vr"].strip()
            opType = data["jhd_opType"].strip()
            result.setdefault((pub, ver), [set(), 0, 0])
            result.setdefault(("all", ver), [set(), 0, 0])
            result.setdefault((pub, "all"), [set(), 0, 0])
            result.setdefault(("all", "all"), [set(), 0, 0])
            if opType in set(["pushaccess"]):
                return False
            if datatype in ["caiyu_ad", "caiyu_ios_free"]:
                if opType == "action":
                    if data["jhd_eventId"].strip() == "ac57":
                        return False

            # if opType == "action":
            #     if data["jhd_eventId"].strip() == "ac57":
            #         return False
            # 计算活跃用户（分版本、渠道）
            result[(pub, ver)][0].add(uid)
            result[(pub, "all")][0].add(uid)
            result[("all", ver)][0].add(uid)
            result[("all", "all")][0].add(uid)
            # 计算启动次数（分版本、渠道）
            if opType == "in":
                result[(pub, ver)][1] += 1
                result[("all", ver)][1] += 1
                result[(pub, "all")][1] += 1
                result[("all", "all")][1] += 1
            # 计算PV（分版本、渠道）
            elif opType == "page":
                result[(pub, ver)][2] += 1
                result[("all", ver)][2] += 1
                result[(pub, "all")][2] += 1
                result[("all", "all")][2] += 1
            return True
        except:
            import traceback
            print(traceback.print_exc())
            return False

    def transformresult(self, analysisresult, *args, **kwargs):
        result = analysisresult.result
        analysisresult.transformresult = result

