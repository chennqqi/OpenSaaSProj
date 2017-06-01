# -*- coding: utf-8 -*-
from Tongji.AnalysisMap.AnalysisMap import AnalysisMap
import json

class AnalysisAdditionOverall(AnalysisMap):

    def __init__(self):
        super(AnalysisAdditionOverall, self).__init__()
        pass

    def transformresult(self, analysisresult, *args, **kwargs):
        result = analysisresult.result
        result_transform = {}
        for key in result:
            ver, pub = key
            searchpv, searchuv = result[key][0], len(result[key][1])
            bookpv, bookuv = result[key][2], len(result[key][3])
            paypv, payuv = result[key][4], len(result[key][5])
            result_transform.setdefault(key, [searchpv, searchuv, bookpv, bookuv, paypv, payuv])
        analysisresult.transformresult = result_transform

    def rules(self, analysisresult, data, num, *args, **kwargs):
        result = analysisresult.result
        # 初始化：搜索次数、人数（ac36），预订次数、人数（ac49），付款成功次数、人数（ac55）
        items = data.split("\t")
        uid = items[0].strip()
        eventdata = json.loads(items[13].strip())
        comepub = items[2].strip()
        pub = comepub
        vers = items[5].strip()

        for ver in vers.split("#"):
            # 初始化：搜索次数、人数（ac36），预订次数、人数（ac49），付款成功次数、人数（ac55）
            result.setdefault((ver, pub), [0, 0, 0, 0, 0, 0])
            result.setdefault(("all", pub), [0, 0, 0, 0, 0, 0])
            result.setdefault((ver, "all"), [0, 0, 0, 0, 0, 0])
            result.setdefault(("all", "all"), [0, 0, 0, 0, 0, 0])

        if "ac36" in eventdata:
            num = eventdata["ac36"]
            for ver in vers.split("#"):
                result[(ver, pub)][0] += num
                result[("all", pub)][0] += num
                result[(ver, "all")][0] += num
                result[("all", "all")][0] += num
                result[(ver, pub)][1] += 1
                result[("all", pub)][1] += 1
                result[(ver, "all")][1] += 1
                result[("all", "all")][1] += 1
        elif "ac49" in eventdata:
            num = eventdata["ac49"]
            for ver in vers.split("#"):
                result[(ver, pub)][2] += num
                result[("all", pub)][2] += num
                result[(ver, "all")][2] += num
                result[("all", "all")][2] += num
                result[(ver, pub)][3] += 1
                result[("all", pub)][3] += 1
                result[(ver, "all")][3] += 1
                result[("all", "all")][3] += 1
        elif "ac55" in eventdata:
            num = eventdata["ac55"]
            for ver in vers.split("#"):
                result[(ver, pub)][4] += num
                result[("all", pub)][4] += num
                result[(ver, "all")][4] += num
                result[("all", "all")][4] += num
                result[(ver, pub)][5] += 1
                result[("all", pub)][5] += 1
                result[(ver, "all")][5] += 1
                result[("all", "all")][5] += 1
        return result

    def rules_bak(self, analysisresult, data):
        result = analysisresult.result
        eventid = data["jhd_eventId"]
        uid = data["jhd_userkey"]
        ver = data["jhd_vr"]
        pub = data["jhd_pb"]
        # 初始化：搜索次数、人数（ac36），预订次数、人数（ac49），付款成功次数、人数（ac55）
        result.setdefault((ver, pub), [0, set(), 0, set(), 0, set()])
        result.setdefault(("all", pub), [0, set(), 0, set(), 0, set()])
        result.setdefault((ver, "all"), [0, set(), 0, set(), 0, set()])
        result.setdefault(("all", "all"), [0, set(), 0, set(), 0, set()])
        if eventid == "ac36":
            result[(ver, pub)][0] += 1
            result[("all", pub)][0] += 1
            result[(ver, "all")][0] += 1
            result[("all", "all")][0] += 1
            result[(ver, pub)][1].add(uid)
            result[("all", pub)][1].add(uid)
            result[(ver, "all")][1].add(uid)
            result[("all", "all")][1].add(uid)
        elif eventid == "ac49":
            result[(ver, pub)][2] += 1
            result[("all", pub)][2] += 1
            result[(ver, "all")][2] += 1
            result[("all", "all")][2] += 1
            result[(ver, pub)][3].add(uid)
            result[("all", pub)][3].add(uid)
            result[(ver, "all")][3].add(uid)
            result[("all", "all")][3].add(uid)
        elif eventid == "ac55":
            result[(ver, pub)][4] += 1
            result[("all", pub)][4] += 1
            result[(ver, "all")][4] += 1
            result[("all", "all")][4] += 1
            result[(ver, pub)][5].add(uid)
            result[("all", pub)][5].add(uid)
            result[(ver, "all")][5].add(uid)
            result[("all", "all")][5].add(uid)