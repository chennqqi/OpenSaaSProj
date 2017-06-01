# -*- coding: utf-8 -*-
from Tongji.AnalysisMap.AnalysisMap import AnalysisMap
import json
import time

class AnalysisInDistribute(AnalysisMap):

    def __init__(self):
        self.finished = False

    def rules(self, analysisresult, data, num, *args, **kwargs):
        result = analysisresult.result
        try:
            items = data.split("\t")
            # uid = items[0].strip()
            isactive = items[1].strip()
            if isactive != "1":
                return False
            vers = items[5].strip()
            comepub = items[2].strip()
            in_num = int(items[11].strip()) if items[11].strip() != "#" else 1  # 没有收到end日志的用户，启动次数设置为1
            is_allver = True
            for ver in vers.split("#"):
                result.setdefault((ver, comepub), [0, 0, 0, {}])  # 日活（uv）、启动人数、启动次数、启动分布
                result.setdefault(("all", comepub), [0, 0, 0, {}])
                result.setdefault((ver, "all"), [0, 0, 0, {}])
                result.setdefault(("all", "all"), [0, 0, 0, {}])

                # 计算日活
                result[(ver, comepub)][0] += 1
                result[("all", comepub)][0] += 1 if is_allver else 0
                result[(ver, "all")][0] += 1
                result[("all", "all")][0] += 1 if is_allver else 0

                # 启动人数
                result[(ver, comepub)][1] += 1
                result[("all", comepub)][1] += 1 if is_allver else 0
                result[(ver, "all")][1] += 1
                result[("all", "all")][1] += 1 if is_allver else 0

                # 启动次数
                result[(ver, comepub)][2] += in_num
                result[("all", comepub)][2] += in_num if is_allver else 0
                result[(ver, "all")][2] += in_num
                result[("all", "all")][2] += in_num if is_allver else 0

                if in_num <= 10 and in_num >= 1:
                    result[(ver, comepub)][3].setdefault("uv_%d" % in_num, 0)
                    result[("all", comepub)][3].setdefault("uv_%d" % in_num, 0)
                    result[(ver, "all")][3].setdefault("uv_%d" % in_num, 0)
                    result[("all", "all")][3].setdefault("uv_%d" % in_num, 0)
                    result[(ver, comepub)][3]["uv_%d" % in_num] += 1
                    result[("all", comepub)][3]["uv_%d" % in_num] += 1 if is_allver else 0
                    result[(ver, "all")][3]["uv_%d" % in_num] += 1
                    result[("all", "all")][3]["uv_%d" % in_num] += 1 if is_allver else 0
                elif in_num > 10:
                    result[(ver, comepub)][3].setdefault("uv_gt10", 0)
                    result[("all", comepub)][3].setdefault("uv_gt10", 0)
                    result[(ver, "all")][3].setdefault("uv_gt10", 0)
                    result[("all", "all")][3].setdefault("uv_gt10", 0)
                    result[(ver, comepub)][3]["uv_gt10"] += 1
                    result[("all", comepub)][3]["uv_gt10"] += 1 if is_allver else 0
                    result[(ver, "all")][3]["uv_gt10"] += 1
                    result[("all", "all")][3]["uv_gt10"] += 1 if is_allver else 0
                is_allver = False
            return True
        except:
            import traceback
            print(traceback.print_exc())
            return False

    def transformresult(self, analysisresult, *args, **kwargs):
        result = analysisresult.result
        analysisresult.transformresult = result

