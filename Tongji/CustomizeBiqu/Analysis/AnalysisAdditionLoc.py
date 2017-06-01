# -*- coding: utf-8 -*-
from Tongji.AnalysisMap.AnalysisMap import AnalysisMap
import json
import time

class AnalysisAdditionLoc(AnalysisMap):

    def __init__(self):
        super(AnalysisAdditionLoc, self).__init__()
        pass

    def assignment(self, result, prov, city, ver, comepub, index1, index2, value):
        result[(prov, city, ver, comepub)][index1][index2] += 1
        result[(prov, city, "all", comepub)][index1][index2] += 1
        result[(prov, city, ver, "all")][index1][index2] += 1
        result[(prov, city, "all", "all")][index1][index2] += 1

        result[(prov, city, ver, comepub)][index1][index2+1] += value
        result[(prov, city, "all", comepub)][index1][index2+1] += value
        result[(prov, city, ver, "all")][index1][index2+1] += value
        result[(prov, city, "all", "all")][index1][index2+1] += value

        result[(prov, "all", ver, comepub)][index1][index2] += 1
        result[(prov, "all", "all", comepub)][index1][index2] += 1
        result[(prov, "all", ver, "all")][index1][index2] += 1
        result[(prov, "all", "all", "all")][index1][index2] += 1

        result[(prov, "all", ver, comepub)][index1][index2+1] += value
        result[(prov, "all", "all", comepub)][index1][index2+1] += value
        result[(prov, "all", ver, "all")][index1][index2+1] += value
        result[(prov, "all", "all", "all")][index1][index2+1] += value

        result[(prov, city, ver, comepub)][index1][index2] += 1
        result[(prov, city, "all", comepub)][index1][index2] += 1
        result[(prov, city, ver, "all")][index1][index2] += 1
        result[(prov, city, "all", "all")][index1][index2] += 1

        result[(prov, city, ver, comepub)][index1][index2+1] += value
        result[(prov, city, "all", comepub)][index1][index2+1] += value
        result[(prov, city, ver, "all")][index1][index2+1] += value
        result[(prov, city, "all", "all")][index1][index2+1] += value

        result[(prov, city, ver, comepub)][index1][index2] += 1
        result[(prov, city, "all", comepub)][index1][index2] += 1
        result[(prov, city, ver, "all")][index1][index2] += 1
        result[(prov, city, "all", "all")][index1][index2] += 1

        result[(prov, city, ver, comepub)][index1][index2+1] += value
        result[(prov, city, "all", comepub)][index1][index2+1] += value
        result[(prov, city, ver, "all")][index1][index2+1] += value
        result[(prov, city, "all", "all")][index1][index2+1] += value

    def rules(self, analysisresult, data, num, *args, **kwargs):
        result = analysisresult.result
        curDay = time.strftime("%Y%m%d", time.localtime(time.time() - 86400 * num))
        items = data.split("\t")
        eventdata = json.loads(items[13].strip())
        comepub = items[2].strip()
        pub = comepub
        vers = items[5].strip()
        ### major key head ###
        loc = items[10].strip()
        isnewcomer = True if curDay in items[8][:8] else False
        # 初始化：搜索次数、人数（ac36），预订次数、人数（ac49），付款成功次数、人数（ac55）
        for ver in vers.split("#"):
            for item in loc.split("#"):
                prov, city = item.split("_")
                if u"省" in city or u"国" in city or u"区" in city or u"洲" in city:
                    continue
                if u"洲" in prov:
                    continue
                result.setdefault((prov, city, ver, comepub), [[0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0]])
                result.setdefault((prov, city, "all", comepub), [[0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0]])
                result.setdefault((prov, city, ver, "all"), [[0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0]])
                result.setdefault((prov, city, "all", "all"), [[0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0]])

                result.setdefault((prov, "all", ver, comepub), [[0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0]])
                result.setdefault((prov, "all", "all", comepub), [[0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0]])
                result.setdefault((prov, "all", ver, "all"), [[0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0]])
                result.setdefault((prov, "all", "all", "all"), [[0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0]])

                result.setdefault((prov, city, ver, comepub), [[0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0]])
                result.setdefault((prov, city, "all", comepub), [[0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0]])
                result.setdefault((prov, city, ver, "all"), [[0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0]])
                result.setdefault((prov, city, "all", "all"), [[0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0]])

                result.setdefault((prov, city, ver, comepub), [[0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0]])
                result.setdefault((prov, city, "all", comepub), [[0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0]])
                result.setdefault((prov, city, ver, "all"), [[0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0]])
                result.setdefault((prov, city, "all", "all"), [[0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0]])

        if "ac36" in eventdata:
            num = eventdata["ac36"]
            for ver in vers.split("#"):
                for item in loc.split("#"):
                    prov, city = item.split("_")
                    if u"省" in city or u"国" in city or u"区" in city or u"洲" in city:
                        continue
                    if u"洲" in prov:
                        continue
                    self.assignment(result, prov, city, ver, pub, 0, 0, num)

        elif "ac49" in eventdata:
            num = eventdata["ac49"]
            for ver in vers.split("#"):
                for item in loc.split("#"):
                    prov, city = item.split("_")
                    if u"省" in city or u"国" in city or u"区" in city or u"洲" in city:
                        continue
                    if u"洲" in prov:
                        continue
                    self.assignment(result, prov, city, ver, pub, 0, 2, num)

        elif "ac55" in eventdata:
            num = eventdata["ac55"]
            for ver in vers.split("#"):
                for item in loc.split("#"):
                    prov, city = item.split("_")
                    if u"省" in city or u"国" in city or u"区" in city or u"洲" in city:
                        continue
                    if u"洲" in prov:
                        continue
                    self.assignment(result, prov, city, ver, pub, 0, 4, num)

        if isnewcomer:
            if "ac36" in eventdata:
                num = eventdata["ac36"]
                for ver in vers.split("#"):
                    for item in loc.split("#"):
                        prov, city = item.split("_")
                        if u"省" in city or u"国" in city or u"区" in city or u"洲" in city:
                            continue
                        if u"洲" in prov:
                            continue
                        self.assignment(result, prov, city, ver, pub, 1, 0, num)

            elif "ac49" in eventdata:
                num = eventdata["ac49"]
                for ver in vers.split("#"):
                    for item in loc.split("#"):
                        prov, city = item.split("_")
                        if u"省" in city or u"国" in city or u"区" in city or u"洲" in city:
                            continue
                        if u"洲" in prov:
                            continue
                        self.assignment(result, prov, city, ver, pub, 1, 2, num)

            elif "ac55" in eventdata:
                num = eventdata["ac55"]
                for ver in vers.split("#"):
                    for item in loc.split("#"):
                        prov, city = item.split("_")
                        if u"省" in city or u"国" in city or u"区" in city or u"洲" in city:
                            continue
                        if u"洲" in prov:
                            continue
                        self.assignment(result, prov, city, ver, pub, 1, 4, num)
        return result


    def transformresult(self, analysisresult):
        result = analysisresult.result
        analysisresult.transformresult = result