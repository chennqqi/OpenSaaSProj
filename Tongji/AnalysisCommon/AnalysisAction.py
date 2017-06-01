# -*- coding: utf-8 -*-
from Tongji.AnalysisMap.AnalysisMap import AnalysisMap
import json
import time

class AnalysisAction(AnalysisMap):

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
            actionDict = json.loads(items[13].strip())
            ### major key tail ###
            isnewcomer = True if curDay in items[8][:8] else False
            for action in actionDict:
                is_allver = True
                for ver in vers.split("#"):
                    action_num = actionDict[action]
                    # 日活
                    # 使用人数
                    result.setdefault((action, ver, comepub), [[0, 0, 0], [0, 0, 0]])[0][0] += 1
                    result.setdefault((action, "all", comepub), [[0, 0, 0], [0, 0, 0]])[0][0] += 1 if is_allver else 0
                    result.setdefault((action, ver, "all"), [[0, 0, 0], [0, 0, 0]])[0][0] += 1
                    result.setdefault((action, "all", "all"), [[0, 0, 0], [0, 0, 0]])[0][0] += 1 if is_allver else 0
                    # 使用次数
                    result.setdefault((action, ver, comepub), [[0, 0, 0], [0, 0, 0]])[0][1] += action_num
                    result.setdefault((action, "all", comepub), [[0, 0, 0], [0, 0, 0]])[0][1] += action_num if is_allver else 0
                    result.setdefault((action, ver, "all"), [[0, 0, 0], [0, 0, 0]])[0][1] += action_num
                    result.setdefault((action, "all", "all"), [[0, 0, 0], [0, 0, 0]])[0][1] += action_num if is_allver else 0

                    # 第三个为保留位

                    # 新增
                    if isnewcomer:
                        # 使用人数
                        result.setdefault((action, ver, comepub), [[0, 0, 0], [0, 0, 0]])[1][0] += 1
                        result.setdefault((action, "all", comepub), [[0, 0, 0], [0, 0, 0]])[1][0] += 1 if is_allver else 0
                        result.setdefault((action, ver, "all"), [[0, 0, 0], [0, 0, 0]])[1][0] += 1
                        result.setdefault((action, "all", "all"), [[0, 0, 0], [0, 0, 0]])[1][0] += 1 if is_allver else 0
                        # 使用次数
                        result.setdefault((action, ver, comepub), [[0, 0, 0], [0, 0, 0]])[1][1] += action_num
                        result.setdefault((action, "all", comepub), [[0, 0, 0], [0, 0, 0]])[1][1] += action_num if is_allver else 0
                        result.setdefault((action, ver, "all"), [[0, 0, 0], [0, 0, 0]])[1][1] += action_num
                        result.setdefault((action, "all", "all"), [[0, 0, 0], [0, 0, 0]])[1][1] += action_num if is_allver else 0
                        # 第三个为保留位
                    is_allver = False
            return True
        except:
            import traceback
            print(traceback.print_exc())
            return False

    def transformresult(self, analysisresult, *args, **kwargs):
        result = analysisresult.result
        analysisresult.transformresult = result

