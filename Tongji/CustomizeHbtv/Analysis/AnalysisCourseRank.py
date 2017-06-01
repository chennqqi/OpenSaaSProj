# -*- coding: utf-8 -*-
import __init__
import time
import json
from Tongji.AnalysisMap.AnalysisMap import AnalysisMap


class AnalysisCourseRank(AnalysisMap):

    def __init__(self):
        super(AnalysisCourseRank, self).__init__()
        self.finished = False

    # 计算新增、活跃、启动、PV（分版本/渠道）
    def rules(self, analysisresult, data, num, *args, **kwargs):
        result = analysisresult.result
        # curDay = time.strftime("%Y%m%d", time.localtime(time.time() - 86400 * num))
        try:
            data = data if isinstance(data, dict) else json.loads(data)
            eventid = data["jhd_eventId"].strip()
            uid = data["jhd_userkey"].strip()
            if eventid == "ac18":
                map_data = data["jhd_map"]
                ynum = float(map_data["ynum"])
                title = map_data["id"]
                result.setdefault(title, [set(), 0, []])
                result[title][0].add(uid)
                result[title][1] += 1
                result[title][2].append(ynum)
                return True
            return False
        except:
            return False

    def transformresult(self, analysisresult, *args, **kwargs):
        result = analysisresult.result
        analysisresult.transformresult = result