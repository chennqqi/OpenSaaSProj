# -*- coding: utf-8 -*-
from Tongji.AnalysisMap.AnalysisMap import AnalysisMap
import json
import time

class AnalysisAdditionUA(AnalysisMap):

    def __init__(self):
        super(AnalysisAdditionUA, self).__init__()
        pass

    def assignment(self, result, ua, ver, pub, index1, index2, value):
        result[(ua, ver, pub)][index1][index2] += 1
        result[(ua, "all", pub)][index1][index2] += 1
        result[(ua, ver, "all")][index1][index2] += 1
        result[(ua, "all", "all")][index1][index2] += 1

        result[(ua, ver, pub)][index1][index2+1] += value
        result[(ua, "all", pub)][index1][index2+1] += value
        result[(ua, ver, "all")][index1][index2+1] += value
        result[(ua, "all", "all")][index1][index2+1] += value

    def rules(self, analysisresult, data, *args, **kwargs):
        result = analysisresult.result
        items = data.split("\t")
        isactive = items[1].strip()
        if isactive != "1":
            return analysisresult
        vers = items[5].strip()
        curpub = items[3].strip()
        comepub = items[2].strip() if items[2].strip() != "#" else curpub.split("#")[0]
        pub = comepub
        eventdata = json.loads(items[13].strip())
        ### major key head ###
        ua = items[6].strip()
        curDay = time.strftime("%Y%m%d", time.localtime(time.time() - 86400 * num))
        isnewcomer = True if curDay in items[8][:8] else False
        # 初始化：搜索次数、人数（ac36），预订次数、人数（ac49），付款成功次数、人数（ac55）
        for ver in vers.split("#"):
            # 初始化：搜索次数、人数（ac36），预订次数、人数（ac49），付款成功次数、人数（ac55）
            result.setdefault((ua, ver, pub), [[0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0]])
            result.setdefault((ua, "all", pub), [[0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0]])
            result.setdefault((ua, ver, "all"), [[0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0]])
            result.setdefault((ua, "all", "all"), [[0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0]])

        if "ac36" in eventdata:
            num = eventdata["ac36"]
            for ver in vers.split("#"):
                self.assignment(result, ua, ver, pub, 0, 0, num)
        elif "ac49" in eventdata:
            num = eventdata["ac49"]
            self.assignment(result, ua, ver, pub, 0, 2, num)
        elif "ac55" in eventdata:
            num = eventdata["ac49"]
            self.assignment(result, ua, ver, pub, 0, 4, num)



    def transformresult(self, analysisresult, num, *args, **kwargs):
        pass