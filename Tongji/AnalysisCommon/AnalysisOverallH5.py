# -*- coding: utf-8 -*-
from Tongji.AnalysisMap.AnalysisMap import AnalysisMap
import json

class AnalysisOverallH5(AnalysisMap):

    def __init__(self):
        self.finished = False

    def rules(self, analysisresult, data, num, *args, **kwargs):
        result = analysisresult.result
        # format: [总用户数, 页面次数，页面人数，触发事件次数，触发事件人数]
        result.setdefault("all", [set(), 0, set(), 0, set()])
        try:
            data = json.loads(data) if not isinstance(data, dict) else data
            uid = data["uid"]
            opatype = data["type"]
            result["all"][0].add(uid)
            # uri = data["uri"]
            # try:
            #     uri = data["uri"]
            #     if uri.startswith("file:/"):
            #         return True
            # except:
            #     pass
            if opatype == "page":
                result["all"][1] += 1
                result["all"][2].add(uid)
            elif opatype == "ac":
                result["all"][3] += 1
                result["all"][4].add(uid)
            return True
        except:
            import traceback
            print(traceback.print_exc())
            return False

    def transformresult(self, analysisresult, *args, **kwargs):
        result = analysisresult.result
        analysisresult.transformresult = result

