# -*- coding: utf-8 -*-
import __init__
from Tongji.AnalysisMap.AnalysisMap import AnalysisMap

class AnalysisSearchSubscribe(AnalysisMap):

    def __init__(self):
        super(AnalysisSearchSubscribe, self).__init__()
        self.actions = set(["ac49", "ac36"])

    def transformresult(self, analysisresult, *args, **kwargs):
        result = analysisresult.result
        result_transform = {}
        for key in result:
            ver, pub = key[0], key[1]
            searchall = len(result[key][0]|result[key][1])
            searchsingle = len(result[key][0])
            searchround = len(result[key][1])
            bookall = len(result[key][2]|result[key][3])
            booksingle = len(result[key][2])
            bookround = len(result[key][3])
            searchsingle_booksingle = len(result[key][0]&result[key][2])
            searchsingle_bookround = len(result[key][0]&result[key][3])
            searchround_booksingle = len(result[key][1]&result[key][2])
            searchround_bookround = len(result[key][1]&result[key][3])
            result_transform.setdefault((ver, pub), (searchall, searchsingle, searchround, bookall, booksingle, bookround, \
                                                     searchsingle_booksingle, searchsingle_bookround, \
                                                     searchround_booksingle, searchround_bookround))
        analysisresult.transformresult = result_transform

    def rules(self, analysisresult, data, num, *args, **kwargs):
        result = analysisresult.result
        eventid = data["jhd_eventId"]
        # if eventid not in self.actions:
        #     return result
        # 搜索用户数据
        if eventid == "ac36":
            uid = data["jhd_userkey"]
            mapinfo = data["jhd_map"]
            ver = data["jhd_vr"]
            pub = data["jhd_pb"]
            if not mapinfo:
                return result
            # og 始发地
            # dt 目的地
            # st 去程时间
            # et 返程时间
            # og = mapinfo["og"]
            # dest = mapinfo["og"]
            # st = mapinfo["st"].replace("-", "") if mapinfo.get("st", None) else None
            et = mapinfo["et"].replace("-", "") if mapinfo.get("et", None) else None
            # 初始化：搜索单程用户，搜索往返用户，预订单程用户，预订往返用户
            result.setdefault((ver, pub), [set(), set(), set(), set()])
            result.setdefault(("all", pub), [set(), set(), set(), set()])
            result.setdefault((ver, "all"), [set(), set(), set(), set()])
            result.setdefault(("all", "all"), [set(), set(), set(), set()])
            if not et:
                result[(ver, pub)][0].add(uid)
                result[("all", pub)][0].add(uid)
                result[(ver, "all")][0].add(uid)
                result[("all", "all")][0].add(uid)
            else:
                result[(ver, pub)][1].add(uid)
                result[("all", pub)][1].add(uid)
                result[(ver, "all")][1].add(uid)
                result[("all", "all")][1].add(uid)
        # 预订用户数据
        elif eventid == "ac49":
            uid = data["jhd_userkey"]
            mapinfo = data["jhd_map"]
            ver = data["jhd_vr"]
            pub = data["jhd_pb"]
            isgoback = True if int(mapinfo["wf"]) == 1 else False
            # 初始化：搜索单程用户，搜索往返用户，预订单程用户，预订往返用户
            result.setdefault((ver, pub), [set(), set(), set(), set()])
            result.setdefault(("all", pub), [set(), set(), set(), set()])
            result.setdefault((ver, "all"), [set(), set(), set(), set()])
            result.setdefault(("all", "all"), [set(), set(), set(), set()])
            if not isgoback:
                result[(ver, pub)][2].add(uid)
                result[("all", pub)][2].add(uid)
                result[(ver, "all")][2].add(uid)
                result[("all", "all")][2].add(uid)
            else:
                result[(ver, pub)][3].add(uid)
                result[("all", pub)][3].add(uid)
                result[(ver, "all")][3].add(uid)
                result[("all", "all")][3].add(uid)
        return result
