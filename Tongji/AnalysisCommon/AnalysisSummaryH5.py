# -*- coding: utf-8 -*-
from Tongji.AnalysisMap.AnalysisMap import AnalysisMap
from SaaSTools.IPtoLoc.iploc_demo import getLoc
import json
import re


class AnalysisSummaryH5(AnalysisMap):

    def __init__(self):
        self.pattern_ua = re.compile(r''';\s*?(\S*?)\s*?Build/''', re.I)
        self.finished = False

    def rules(self, analysisresult, data, num, *args, **kwargs):
        result = analysisresult.result
        try:
            data = json.loads(data) if not isinstance(data, dict) else data
            uid = data["uid"]
            ip = data["ip"].strip('''"''')
            opatype = data["type"]
            prov, city = getLoc(ip)
            prov = prov.decode("utf-8")
            city = city.decode("utf-8")
            try:
                device = data["device"]["name"].lower()
            except:
                device = "unkown"
            if "android" in device:
                ua = data["ua"]
                m = self.pattern_ua.search(ua)
                if m:
                    device = m.group(1)
                    device = device.lower()
            try:
                browser = data["browser"]["name"].lower()
            except:
                browser = "unkown"

            result.setdefault(("all", "all", "all", "all"), [set(), 0, set(), 0, set(), 0, 0, set()])[0].add(uid)

            result.setdefault((prov, "all", "all", "all"), [set(), 0, set(), 0, set(), 0, 0, set()])[0].add(uid)
            result.setdefault((prov, city, "all", "all"), [set(), 0, set(), 0, set(), 0, 0, set()])[0].add(uid)
            result.setdefault((prov, "all", device, "all"), [set(), 0, set(), 0, set(), 0, 0, set()])[0].add(uid)
            result.setdefault((prov, "all", "all", browser), [set(), 0, set(), 0, set(), 0, 0, set()])[0].add(uid)

            result.setdefault(("all", city, "all", "all"), [set(), 0, set(), 0, set(), 0, 0, set()])[0].add(uid)
            result.setdefault(("all", city, device, "all"), [set(), 0, set(), 0, set(), 0, 0, set()])[0].add(uid)
            result.setdefault(("all", city, "all", browser), [set(), 0, set(), 0, set(), 0, 0, set()])[0].add(uid)

            result.setdefault(("all", "all", device, "all"), [set(), 0, set(), 0, set(), 0, 0, set()])[0].add(uid)
            result.setdefault(("all", "all", device, browser), [set(), 0, set(), 0, set(), 0, 0, set()])[0].add(uid)

            result.setdefault(("all", "all", "all", browser), [set(), 0, set(), 0, set(), 0, 0, set()])[0].add(uid)

            if opatype == "page":
                # pagepv
                result[("all", "all", "all", "all")][1] += 1

                result[(prov, "all", "all", "all")][1] += 1
                result[(prov, city, "all", "all")][1] += 1
                result[(prov, "all", device, "all")][1] += 1
                result[(prov, "all", "all", browser)][1] += 1

                result[("all", city, "all", "all")][1] += 1
                result[("all", city, device, "all")][1] += 1
                result[("all", city, "all", browser)][1] += 1

                result[("all", "all", device, "all")][1] += 1
                result[("all", "all", device, browser)][1] += 1

                result[("all", "all", "all", browser)][1] += 1

                # pageuv
                result[("all", "all", "all", "all")][2].add(uid)

                result[(prov, "all", "all", "all")][2].add(uid)
                result[(prov, city, "all", "all")][2].add(uid)
                result[(prov, "all", device, "all")][2].add(uid)
                result[(prov, "all", "all", browser)][2].add(uid)

                result[("all", city, "all", "all")][2].add(uid)
                result[("all", city, device, "all")][2].add(uid)
                result[("all", city, "all", browser)][2].add(uid)

                result[("all", "all", device, "all")][2].add(uid)
                result[("all", "all", device, browser)][2].add(uid)

                result[("all", "all", "all", browser)][2].add(uid)

            elif opatype == "ac":
                # pagepv
                result[("all", "all", "all", "all")][3] += 1

                result[(prov, "all", "all", "all")][3] += 1
                result[(prov, city, "all", "all")][3] += 1
                result[(prov, "all", device, "all")][3] += 1
                result[(prov, "all", "all", browser)][3] += 1

                result[("all", city, "all", "all")][3] += 1
                result[("all", city, device, "all")][3] += 1
                result[("all", city, "all", browser)][3] += 1

                result[("all", "all", device, "all")][3] += 1
                result[("all", "all", device, browser)][3] += 1

                result[("all", "all", "all", browser)][3] += 1

                # pageuv
                result[("all", "all", "all", "all")][4].add(uid)

                result[(prov, "all", "all", "all")][4].add(uid)
                result[(prov, city, "all", "all")][4].add(uid)
                result[(prov, "all", device, "all")][4].add(uid)
                result[(prov, "all", "all", browser)][4].add(uid)

                result[("all", city, "all", "all")][4].add(uid)
                result[("all", city, device, "all")][4].add(uid)
                result[("all", city, "all", browser)][4].add(uid)

                result[("all", "all", device, "all")][4].add(uid)
                result[("all", "all", device, browser)][4].add(uid)

                result[("all", "all", "all", browser)][4].add(uid)
            elif opatype == "dur":
                dur = float(data["value"]) / 1000
                # 大于30分钟正常页面的停留时长为脏数据，不统计
                # 但对于视频类等文章需要特殊处理
                if dur > 1800 or dur <= 0:
                    return False
                # totaldur
                result[("all", "all", "all", "all")][5] += dur

                result[(prov, "all", "all", "all")][5] += dur
                result[(prov, city, "all", "all")][5] += dur
                result[(prov, "all", device, "all")][5] += dur
                result[(prov, "all", "all", browser)][5] += dur

                result[("all", city, "all", "all")][5] += dur
                result[("all", city, device, "all")][5] += dur
                result[("all", city, "all", browser)][5] += dur

                result[("all", "all", device, "all")][5] += dur
                result[("all", "all", device, browser)][5] += dur

                result[("all", "all", "all", browser)][5] += dur

                # durpv
                result[("all", "all", "all", "all")][6] += 1

                result[(prov, "all", "all", "all")][6] += 1
                result[(prov, city, "all", "all")][6] += 1
                result[(prov, "all", device, "all")][6] += 1
                result[(prov, "all", "all", browser)][6] += 1

                result[("all", city, "all", "all")][6] += 1
                result[("all", city, device, "all")][6] += 1
                result[("all", city, "all", browser)][6] += 1

                result[("all", "all", device, "all")][6] += 1
                result[("all", "all", device, browser)][6] += 1

                result[("all", "all", "all", browser)][6] += 1

                # duruv
                result[("all", "all", "all", "all")][7].add(uid)

                result[(prov, "all", "all", "all")][7].add(uid)
                result[(prov, city, "all", "all")][7].add(uid)
                result[(prov, "all", device, "all")][7].add(uid)
                result[(prov, "all", "all", browser)][7].add(uid)

                result[("all", city, "all", "all")][7].add(uid)
                result[("all", city, device, "all")][7].add(uid)
                result[("all", city, "all", browser)][7].add(uid)

                result[("all", "all", device, "all")][7].add(uid)
                result[("all", "all", device, browser)][7].add(uid)

                result[("all", "all", "all", browser)][7].add(uid)
            return True
        except:
            import traceback
            print(traceback.print_exc())
            return False

    def transformresult(self, analysisresult, *args, **kwargs):
        result = analysisresult.result
        analysisresult.transformresult = result