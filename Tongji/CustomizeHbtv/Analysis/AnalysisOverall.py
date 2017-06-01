# -*- coding: utf-8 -*-
import __init__
import time
import json
from Tongji.AnalysisMap.AnalysisMap import AnalysisMap


class AnalysisOverall(AnalysisMap):

    def __init__(self):
        super(AnalysisOverall, self).__init__()
        self.finished = False

    # 计算新增、活跃、启动、PV（分版本/渠道）
    def rules(self, analysisresult, data, num, *args, **kwargs):
        result = analysisresult.result
        curDay = time.strftime("%Y%m%d", time.localtime(time.time() - 86400 * num))
        try:
            items = data.split("\t")
            isactive = True if items[1].strip() == "1" else False
            if not isactive:
                return False
            vers = items[5].strip()
            curpub = items[3].strip()
            comepub = items[2].strip() if items[2].strip() != "#" else curpub.split("#")[0]
            in_num = int(items[11].strip())
            page_data = json.loads(items[14].strip())
            page_num = sum([page_data[key] for key in page_data])
            isnewcomer = True if curDay in items[8][:8] else False
            action_dic = json.loads(items[13])
            events = action_dic.keys()
            play_num = action_dic.get("ac18", 0) + action_dic.get("ac15", 0)
            dur = 0 if items[12] == "#" else int(sum(map(float, items[12].split("#"))))
            dur = 0 if dur >= 21600 else dur
            is_allver = True
            for ver in vers.split("#"):
                # 日活
                result.setdefault((ver, comepub), [[0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0, 0]])[0][0] += 1
                result.setdefault(("all", comepub), [[0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0, 0]])[0][0] += 1 if is_allver else 0
                result.setdefault((ver, "all"), [[0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0, 0]])[0][0] += 1
                result.setdefault(("all", "all"), [[0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0, 0]])[0][0] += 1 if is_allver else 0
                # 启动次数
                result.setdefault((ver, comepub), [[0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0, 0]])[0][1] += in_num
                result.setdefault(("all", comepub), [[0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0, 0]])[0][1] += in_num if is_allver else 0
                result.setdefault((ver, "all"), [[0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0, 0]])[0][1] += in_num
                result.setdefault(("all", "all"), [[0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0, 0]])[0][1] += in_num if is_allver else 0
                # 页面
                result.setdefault((ver, comepub), [[0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0, 0]])[0][2] += page_num
                result.setdefault(("all", comepub), [[0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0, 0]])[0][2] += page_num if is_allver else 0
                result.setdefault((ver, "all"), [[0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0, 0]])[0][2] += page_num
                result.setdefault(("all", "all"), [[0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0, 0]])[0][2] += page_num if is_allver else 0
                # 使用时长
                result.setdefault((ver, comepub), [[0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0, 0]])[0][3] += dur
                result.setdefault(("all", comepub), [[0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0, 0]])[0][3] += dur if is_allver else 0
                result.setdefault((ver, "all"), [[0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0, 0]])[0][3] += dur
                result.setdefault(("all", "all"), [[0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0, 0]])[0][3] += dur if is_allver else 0
                # 播放次数
                result.setdefault((ver, comepub), [[0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0, 0]])[0][6] += play_num
                result.setdefault(("all", comepub), [[0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0, 0]])[0][6] += play_num if is_allver else 0
                result.setdefault((ver, "all"), [[0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0, 0]])[0][6] += play_num
                result.setdefault(("all", "all"), [[0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0, 0]])[0][6] += play_num if is_allver else 0

                if "ac18" in events:
                    result.setdefault((ver, comepub), [[0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0, 0]])[0][4] += 1
                    result.setdefault(("all", comepub), [[0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0, 0]])[0][4] += 1 if is_allver else 0
                    result.setdefault((ver, "all"), [[0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0, 0]])[0][4] += 1
                    result.setdefault(("all", "all"), [[0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0, 0]])[0][4] += 1 if is_allver else 0

                if "ac15" in events or "ac18" in events:
                    result.setdefault((ver, comepub), [[0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0, 0]])[0][5] += 1
                    result.setdefault(("all", comepub), [[0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0, 0]])[0][5] += 1 if is_allver else 0
                    result.setdefault((ver, "all"), [[0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0, 0]])[0][5] += 1
                    result.setdefault(("all", "all"), [[0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0, 0]])[0][5] += 1 if is_allver else 0

                # 新增
                if isnewcomer:
                    # 新增
                    result.setdefault((ver, comepub), [[0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0, 0]])[1][0] += 1
                    result.setdefault(("all", comepub), [[0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0, 0]])[1][0] += 1 if is_allver else 0
                    result.setdefault((ver, "all"), [[0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0, 0]])[1][0] += 1
                    result.setdefault(("all", "all"), [[0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0, 0]])[1][0] += 1 if is_allver else 0
                    # 启动次数
                    result.setdefault((ver, comepub), [[0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0, 0]])[1][1] += in_num
                    result.setdefault(("all", comepub), [[0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0, 0]])[1][1] += in_num if is_allver else 0
                    result.setdefault((ver, "all"), [[0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0, 0]])[1][1] += in_num
                    result.setdefault(("all", "all"), [[0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0, 0]])[1][1] += in_num if is_allver else 0
                    # 页面
                    result.setdefault((ver, comepub), [[0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0, 0]])[1][2] += page_num
                    result.setdefault(("all", comepub), [[0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0, 0]])[1][2] += page_num if is_allver else 0
                    result.setdefault((ver, "all"), [[0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0, 0]])[1][2] += page_num
                    result.setdefault(("all", "all"), [[0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0, 0]])[1][2] += page_num if is_allver else 0
                    # 使用时长
                    result.setdefault((ver, comepub), [[0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0, 0]])[1][3] += dur
                    result.setdefault(("all", comepub), [[0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0, 0]])[1][3] += dur if is_allver else 0
                    result.setdefault((ver, "all"), [[0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0, 0]])[1][3] += dur
                    result.setdefault(("all", "all"), [[0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0, 0]])[1][3] += dur if is_allver else 0
                    # 播放次数
                    result.setdefault((ver, comepub), [[0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0, 0]])[1][6] += play_num
                    result.setdefault(("all", comepub), [[0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0, 0]])[1][6] += play_num if is_allver else 0
                    result.setdefault((ver, "all"), [[0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0, 0]])[1][6] += play_num
                    result.setdefault(("all", "all"), [[0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0, 0]])[1][6] += play_num if is_allver else 0
                    if "ac18" in events:
                        result.setdefault((ver, comepub), [[0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0, 0]])[1][4] += 1
                        result.setdefault(("all", comepub), [[0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0, 0]])[1][4] += 1 if is_allver else 0
                        result.setdefault((ver, "all"), [[0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0, 0]])[0][1] += 1
                        result.setdefault(("all", "all"), [[0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0, 0]])[1][4] += 1 if is_allver else 0

                    # 播放 视频、课件
                    if "ac15" in events or "ac18" in events:
                        result.setdefault((ver, comepub), [[0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0, 0]])[1][5] += 1
                        result.setdefault(("all", comepub), [[0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0, 0]])[1][5] += 1 if is_allver else 0
                        result.setdefault((ver, "all"), [[0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0, 0]])[1][5] += 1
                        result.setdefault(("all", "all"), [[0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0, 0]])[1][5] += 1 if is_allver else 0
                is_allver = False
            return True
        except:
            import traceback
            print(traceback.print_exc())

    def transformresult(self, analysisresult, *args, **kwargs):
        result = analysisresult.result
        analysisresult.transformresult = result