# -*- coding: utf-8 -*-
import json
from urlparse import urlparse
from Tongji.AnalysisMap.AnalysisMap import AnalysisMap


class AnalysisPageH5(AnalysisMap):

    def __init__(self):
        self.user_refid = {}
        self.finished = False

    def rules(self, analysisresult, data, num, *args, **kwargs):
        result = analysisresult.result
        try:
            data = json.loads(data) if not isinstance(data, dict) else data
            uid = data["uid"]
            opatype = data["type"]
            if opatype == "page":
                pageid = data["uri"]
                try:
                    if pageid.startswith("file:/"):
                        return False
                except:
                    pass

                uri_parse = urlparse(pageid)
                uri_hostname = uri_parse.netloc

                refid = data["ref"]
                ref_parse = urlparse(refid)
                ref_hostname = ref_parse.hostname

                if uri_hostname != ref_hostname:
                    refid = ""
                else:
                    refid = refid[:254]


                pageid = data["uri"][:254]

                self.user_refid[(uid, pageid)] = refid
                # format: PV, UV, dur, durpv, duruv
                # result.setdefault("all", [0, set(), 0])
                result.setdefault((refid, pageid), [0, set(), 0, 0, set()])
                result.setdefault(("all", pageid), [0, set(), 0, 0, set()])
                result.setdefault((refid, "all"), [0, set(), 0, 0, set()])
                # PV
                result[(refid, pageid)][0] += 1
                result[("all", pageid)][0] += 1
                result[(refid, "all")][0] += 1
                # UV
                result[(refid, pageid)][1].add(uid)
                result[("all", pageid)][1].add(uid)
                result[(refid, "all")][1].add(uid)
            elif opatype == "dur":
                pageid = data["uri"][:254]
                dur = float(data["value"])  # 单位：毫秒
                # 大于30分钟正常页面的停留时长为脏数据，不统计
                # 但对于视频类等文章需要特殊处理
                if dur > 1800 * 1000:
                    return False
                if dur <= 300:
                    return False
                # 毫秒 -> 秒
                dur = dur / 1000.0
                refid = self.user_refid.get((uid, pageid), None)
                if refid == None:
                    # format: PV, UV, dur, durpv, duruv
                    # result.setdefault("all", [0, set(), 0])
                    result.setdefault(("all", pageid), [0, set(), 0, 0, set()])

                    result[("all", pageid)][2] += dur
                    result[("all", pageid)][3] += 1
                    result[("all", pageid)][4].add(uid)
                else:
                    # format: PV, UV, dur, durpv, duruv
                    # result.setdefault("all", [0, set(), 0])
                    result.setdefault((refid, pageid), [0, set(), 0, 0, set()])
                    result.setdefault(("all", pageid), [0, set(), 0, 0, set()])
                    result.setdefault((refid, "all"), [0, set(), 0, 0, set()])
                    # dur
                    result[(refid, pageid)][2] += dur
                    result[("all", pageid)][2] += dur
                    result[(refid, "all")][2] += dur
                    # durpv
                    result[(refid, pageid)][3] += 1
                    result[("all", pageid)][3] += 1
                    result[(refid, "all")][3] += 1
                    # duruv
                    result[(refid, pageid)][4].add(uid)
                    result[("all", pageid)][4].add(uid)
                    result[(refid, "all")][4].add(uid)
            return True
        except:
            import traceback
            print(traceback.print_exc())
            return False

    def transformresult(self, analysisresult, *args, **kwargs):
        result = analysisresult.result
        analysisresult.transformresult = result

