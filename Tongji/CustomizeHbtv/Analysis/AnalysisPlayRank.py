# -*- coding: utf-8 -*-
import __init__
import json
from Tongji.AnalysisMap.AnalysisMap import AnalysisMap


class AnalysisPlayRank(AnalysisMap):

    def __init__(self):
        super(AnalysisPlayRank, self).__init__()
        self.finished = False

    # 计算新增、活跃、启动、PV（分版本/渠道）
    def rules(self, analysisresult, data, num, *args, **kwargs):
        result = analysisresult.result
        # curDay = time.strftime("%Y%m%d", time.localtime(time.time() - 86400 * num))
        try:
            data = data if isinstance(data, dict) else json.loads(data)
            eventid = data["jhd_eventId"].strip()
            uid = data["jhd_userkey"].strip()
            if eventid == "ac15":
                map_data = data["jhd_map"]
                pdur = float(map_data["pdur"])
                adur = float(map_data["adur"])
                title = map_data["id"]
                result.setdefault(title, [set(), 0, []])
                result[title][0].add(uid)
                result[title][1] += 1
                per = pdur / float(adur)
                if per > 1:
                    return False
                result[title][2].append(per)
                return True
            return False
        except:
            return False

    def transformresult(self, analysisresult, *args, **kwargs):
        result = analysisresult.result
        analysisresult.transformresult = result