# -*- coding: utf-8 -*-
from urlparse import urlparse
import json
import sys
reload(sys)
sys.setdefaultencoding("utf-8")
from Tongji.AnalysisMap.AnalysisMap import AnalysisMap


class AnalysisRefSourceH5(AnalysisMap):

    def __init__(self):
        self.user_entrance = {}
        self.finished = False
        self.refname_table = {
            "direct": u"直接访问",
            "www.so.com": u"360搜索",
            "m.so.com": u"360搜索",
            "www.baidu.com": u"百度搜索",
            "m.baidu.com": u"百度搜索",
            "wap.baidu.com": u"百度搜索",
            "www.sogou.com": u"搜狗搜索",
            "m.sogou.com": u"搜狗搜索",
            "wap.sogou.com": u"搜狗搜索",
            "www.2345.com": u"导航网站",
            "hao.360.cn": u"导航网站",
            "www.hao123.com": u"导航网站",
        }

    def rules(self, analysisresult, data, num, *args, **kwargs):
        result = analysisresult.result
        try:
            data = json.loads(data) if not isinstance(data, dict) else data
            optype = data["type"].strip()
            uid = data["uid"].strip()
            if optype == "page":
                uri = data["uri"].strip()
                ref = data["ref"].strip().split("?")[0]
                uri_parse = urlparse(uri)
                ref_parse = urlparse(ref)
                uri_hostname = uri_parse.netloc
                ref_hostname = ref_parse.hostname
                # 判断用户引入方式
                if uri_hostname != ref_hostname:
                    ts = data["ts"]
                    if ref_hostname is None:
                        ref_hostname = "direct"
                        ref = "direct"
                    # format: ref_hostname, ref, last_ts, Visit View, leave rate
                    ref_name = self.refname_table.get(ref_hostname, u"其它")
                    self.user_entrance.setdefault(uid, [(ref_hostname, ref, ref_name), ts, 1, 0])
                # 计算各引入方式数据
                if uid in self.user_entrance:
                    key = self.user_entrance[uid][0]
                    ref_hostname, ref, ref_name = key[0], key[1], key[2]
                    ts = data["ts"]
                    ip = data["ip"]
                    # ref_hostname, ref = self.user_entrance[uid][0][0], self.user_entrance[uid][0][1]
                    # format: PV, UV, IPUV, leave rate(uv), Visit View, dur, durpv, dur uv
                    result.setdefault((ref_hostname, ref, ref_name), [0, set(), set(), 0, 0, 0.0, 0, set()])
                    result.setdefault((ref_hostname, "all", ref_name), [0, set(), set(), 0, 0, 0.0, 0, set()])
                    result.setdefault(("all", "all", ref_name), [0, set(), set(), 0, 0, 0.0, 0, set()])

                    result[(ref_hostname, ref, ref_name)][0] += 1
                    result[(ref_hostname, ref, ref_name)][1].add(uid)
                    result[(ref_hostname, ref, ref_name)][2].add(ip)

                    result[(ref_hostname, "all", ref_name)][0] += 1
                    result[(ref_hostname, "all", ref_name)][1].add(uid)
                    result[(ref_hostname, "all", ref_name)][2].add(ip)

                    result[("all", "all", ref_name)][0] += 1
                    result[("all", "all", ref_name)][1].add(uid)
                    result[("all", "all", ref_name)][2].add(ip)

                    # 计算跳出率
                    self.user_entrance[uid][3] += 1
                    # 访客连续30分钟没有新开，则被计算为一次访问
                    if (ts - self.user_entrance[uid][1]) > 1800000:
                        self.user_entrance[uid][2] += 1
                    self.user_entrance[uid][1] = ts if ts > self.user_entrance[uid][1] else self.user_entrance[uid][1]
            elif optype == "dur":
                if uid in self.user_entrance:
                    dur = float(data["value"])
                    # 大于30分钟正常页面的停留时长为脏数据，不统计
                    # 但对于视频类等文章需要特殊处理
                    if dur > 1800 * 1000:
                        return False
                    if dur <= 300:
                        return False
                    # 毫秒 -> 秒
                    dur = dur / 1000.0
                    ref_hostname, ref, ref_name = self.user_entrance[uid][0][0], self.user_entrance[uid][0][1], self.user_entrance[uid][0][2]

                    result[(ref_hostname, ref, ref_name)][5] += dur
                    result[(ref_hostname, ref, ref_name)][6] += 1
                    result[(ref_hostname, ref, ref_name)][7].add(uid)

                    result[(ref_hostname, "all", ref_name)][5] += dur
                    result[(ref_hostname, "all", ref_name)][6] += 1
                    result[(ref_hostname, "all", ref_name)][7].add(uid)

                    result[("all", "all", ref_name)][5] += dur
                    result[("all", "all", ref_name)][6] += 1
                    result[("all", "all", ref_name)][7].add(uid)
            return True
        except:
            import traceback
            print(traceback.print_exc())
            return False

    def transformresult(self, analysisresult, *args, **kwargs):
        result = analysisresult.result
        for uid in self.user_entrance:
            try:
                ref_hostname, ref, ref_name = self.user_entrance[uid][0]
                result[(ref_hostname, ref, ref_name)][4] += self.user_entrance[uid][2]
                result[(ref_hostname, ref, ref_name)][3] += 1 if self.user_entrance[uid][3] == 1 else 0

                result[(ref_hostname, "all", ref_name)][4] += self.user_entrance[uid][2]
                result[(ref_hostname, "all", ref_name)][3] += 1 if self.user_entrance[uid][3] == 1 else 0

                result[("all", "all", ref_name)][4] += self.user_entrance[uid][2]
                result[("all", "all", ref_name)][3] += 1 if self.user_entrance[uid][3] == 1 else 0
            except:
                # import traceback
                # print(traceback.print_exc())
                pass
        analysisresult.transformresult = result
