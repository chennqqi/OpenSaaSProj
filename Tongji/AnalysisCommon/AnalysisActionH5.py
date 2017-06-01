# -*- coding: utf-8 -*-
from Tongji.AnalysisMap.AnalysisMap import AnalysisMap
import json

class AnalysisActionH5(AnalysisMap):

    def __init__(self):
        self.finished = False


    def rules(self, analysisresult, data, num, *args, **kwargs):
        result = analysisresult.result
        try:
            data = json.loads(data) if not isinstance(data, dict) else data
            uid = data["uid"]
            opatype = data["type"]
            if opatype != "ac":
                return False
            # try:
            #     uri = data["uri"]
            #     if uri.startswith("file:/"):
            #         return True
            # except:
            #     pass
            action_var = data["event"]
            # format: PV, UV
            result.setdefault(action_var, [0, set()])
            result[action_var][0] += 1
            result[action_var][1].add(uid)
            return True
        except:
            import traceback
            print(traceback.print_exc())
            return False

    def transformresult(self, analysisresult, *args, **kwargs):
        result = analysisresult.result
        analysisresult.transformresult = result

