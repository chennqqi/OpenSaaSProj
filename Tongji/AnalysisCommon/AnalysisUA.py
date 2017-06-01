# -*- coding: utf-8 -*-
from Tongji.AnalysisMap.AnalysisMap import AnalysisMap
import json
import time

class AnalysisUA(AnalysisMap):

    def __init__(self):
        self.finished = False


    def rules(self, analysisresult, data, num, *args, **kwargs):
        result = analysisresult.result
        curDay = time.strftime("%Y%m%d", time.localtime(time.time() - 86400 * num))
        try:
            items = data.split("\t")
            isactive = items[1].strip()
            if isactive != "1":
                return False
            vers = items[5].strip()
            curpub = items[3].strip()
            comepub = items[2].strip() if items[2].strip() != "#" else curpub.split("#")[0]
            ### major key head ###
            ua = items[6].strip()
            ### major key tail ###
            in_num = int(items[11].strip())
            page_data = json.loads(items[14].strip())
            page_num = sum([page_data[key] for key in page_data])
            isnewcomer = True if curDay in items[8][:8] else False
            is_allver = True
            for ver in vers.split("#"):
                # 日活
                result.setdefault((ua, ver, comepub), [[0, 0, 0], [0, 0, 0]])[0][0] += 1
                result.setdefault((ua, "all", comepub), [[0, 0, 0], [0, 0, 0]])[0][0] += 1 if is_allver else 0
                result.setdefault((ua, ver, "all"), [[0, 0, 0], [0, 0, 0]])[0][0] += 1
                result.setdefault((ua, "all", "all"), [[0, 0, 0], [0, 0, 0]])[0][0] += 1 if is_allver else 0
                # 启动次数
                result.setdefault((ua, ver, comepub), [[0, 0, 0], [0, 0, 0]])[0][1] += in_num
                result.setdefault((ua, "all", comepub), [[0, 0, 0], [0, 0, 0]])[0][1] += in_num if is_allver else 0
                result.setdefault((ua, ver, "all"), [[0, 0, 0], [0, 0, 0]])[0][1] += in_num
                result.setdefault((ua, "all", "all"), [[0, 0, 0], [0, 0, 0]])[0][1] += in_num if is_allver else 0
                # 页面
                result.setdefault((ua, ver, comepub), [[0, 0, 0], [0, 0, 0]])[0][2] += page_num
                result.setdefault((ua, "all", comepub), [[0, 0, 0], [0, 0, 0]])[0][2] += page_num if is_allver else 0
                result.setdefault((ua, ver, "all"), [[0, 0, 0], [0, 0, 0]])[0][2] += page_num
                result.setdefault((ua, "all", "all"), [[0, 0, 0], [0, 0, 0]])[0][2] += page_num if is_allver else 0

                # 新增
                if isnewcomer:
                    # 新增
                    result.setdefault((ua, ver, comepub), [[0, 0, 0], [0, 0, 0]])[1][0] += 1
                    result.setdefault((ua, "all", comepub), [[0, 0, 0], [0, 0, 0]])[1][0] += 1 if is_allver else 0
                    result.setdefault((ua, ver, "all"), [[0, 0, 0], [0, 0, 0]])[1][0] += 1
                    result.setdefault((ua, "all", "all"), [[0, 0, 0], [0, 0, 0]])[1][0] += 1 if is_allver else 0
                    # 启动次数
                    result.setdefault((ua, ver, comepub), [[0, 0, 0], [0, 0, 0]])[1][1] += in_num
                    result.setdefault((ua, "all", comepub), [[0, 0, 0], [0, 0, 0]])[1][1] += in_num if is_allver else 0
                    result.setdefault((ua, ver, "all"), [[0, 0, 0], [0, 0, 0]])[1][1] += in_num
                    result.setdefault((ua, "all", "all"), [[0, 0, 0], [0, 0, 0]])[1][1] += in_num if is_allver else 0
                    # 页面
                    result.setdefault((ua, ver, comepub), [[0, 0, 0], [0, 0, 0]])[1][2] += page_num
                    result.setdefault((ua, "all", comepub), [[0, 0, 0], [0, 0, 0]])[1][2] += page_num if is_allver else 0
                    result.setdefault((ua, ver, "all"), [[0, 0, 0], [0, 0, 0]])[1][2] += page_num
                    result.setdefault((ua, "all", "all"), [[0, 0, 0], [0, 0, 0]])[1][2] += page_num if is_allver else 0
                is_allver = False
            return True
        except:
            import traceback
            print(traceback.print_exc())
            return False

    def transformresult(self, analysisresult, *args, **kwargs):
        result = analysisresult.result
        analysisresult.transformresult = result

