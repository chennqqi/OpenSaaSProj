# -*- coding: utf-8 -*-
from Tongji.AnalysisMap.AnalysisMap import AnalysisMap
import json

class AnalysisAdditionOS(AnalysisMap):

    def __init__(self):
        super(AnalysisAdditionOS, self).__init__()
        pass

    def rules(self, analysisresult, data, num, *args, **kwargs):
        result = analysisresult.result
        items = data.split("\t")
        # uid = items[0].strip()
        eventdata = json.loads(items[13].strip())
        comepub = items[2].strip()
        pub = comepub
        vers = items[5].strip()
        ### major key head ###
        os = items[4].strip()
        # 初始化：搜索次数、人数（ac36），预订次数、人数（ac49），付款成功次数、人数（ac55）
        for ver in vers.split("#"):
            # 初始化：搜索次数、人数（ac36），预订次数、人数（ac49），付款成功次数、人数（ac55）
            result.setdefault((os, ver, pub), [0, 0, 0, 0, 0, 0])
            result.setdefault((os, "all", pub), [0, 0, 0, 0, 0, 0])
            result.setdefault((os, ver, "all"), [0, 0, 0, 0, 0, 0])
            result.setdefault((os, "all", "all"), [0, 0, 0, 0, 0, 0])

        if "ac36" in eventdata:
            num = eventdata["ac36"]
            for ver in vers.split("#"):
                result[(os, ver, pub)][0] += num
                result[(os, "all", pub)][0] += num
                result[(os, ver, "all")][0] += num
                result[(os, "all", "all")][0] += num
                result[(os, ver, pub)][1] += 1
                result[(os, "all", pub)][1] += 1
                result[(os, ver, "all")][1] += 1
                result[(os, "all", "all")][1] += 1
        elif "ac49" in eventdata:
            num = eventdata["ac49"]
            for ver in vers.split("#"):
                result[(os, ver, pub)][2] += num
                result[(os, "all", pub)][2] += num
                result[(os, ver, "all")][2] += num
                result[(os, "all", "all")][2] += num
                result[(os, ver, pub)][3] += 1
                result[(os, "all", pub)][3] += 1
                result[(os, ver, "all")][3] += 1
                result[(os, "all", "all")][3] += 1
        elif "ac55" in eventdata:
            num = eventdata["ac55"]
            for ver in vers.split("#"):
                result[(os, ver, pub)][4] += num
                result[(os, "all", pub)][4] += num
                result[(os, ver, "all")][4] += num
                result[(os, "all", "all")][4] += num
                result[(os, ver, pub)][5] += 1
                result[(os, "all", pub)][5] += 1
                result[(os, ver, "all")][5] += 1
                result[(os, "all", "all")][5] += 1
        return result

    def transformresult(self, analysisresult, *args, **kwargs):
        result = analysisresult.result
        analysisresult.transformresult = result