# -*- coding: utf-8 -*-
from Tongji.AnalysisMap.AnalysisMap import AnalysisMap
import json
import time

class AnalysisUseTimeDistribute(AnalysisMap):

    def __init__(self):
        self.user_dur_datatype = {}
        self.finished = False

    def rules(self, analysisresult, data, num, *args, **kwargs):
        datatype = kwargs["datatype"]
        curDay = time.strftime("%Y%m%d", time.localtime(time.time() - 86400 * num))
        user_dur = self.user_dur_datatype.get((datatype, curDay), {})
        if user_dur:
            try:
                import usetimeDistributeData
                user_dur = usetimeDistributeData.usetimeDistribute(num, datatype)
            except:
                import traceback
                print(traceback.print_exc())
        result = analysisresult.result
        try:
            items = data.split("\t")
            isactive = items[1].strip()
            if isactive != "1":
                return False
            uid = items[0].strip()
            comepub = items[2].strip()
            vers = items[5].strip()
            durs = items[12].strip().split("#") if items[12].strip() != "#" else []
            is_allver = True
            for ver in vers.split("#"):
                result.setdefault((ver, comepub), [0, 0, 0, {}, 0])
                result.setdefault(("all", comepub), [0, 0, 0, {}, 0])
                result.setdefault((ver, "all"), [0, 0, 0, {}, 0])
                result.setdefault(("all", "all"), [0, 0, 0, {}, 0])

                # 计算日活
                result[(ver, comepub)][0] += 1
                result[("all", comepub)][0] += 1
                result[(ver, "all")][0] += 1
                result[("all", "all")][0] += 1
                durs_lis = [float(dur) for dur in durs if float(dur) < 1800 and float(dur) >= 0]
                dur_total = int(sum(durs_lis)) if uid not in user_dur else user_dur[uid]
                # if uid not in user_dur:
                #     i += 1
                #     print(dur_total)
                # else:
                #     j += 1
                # 排除全天使用时长大于12小时的用户，
                if dur_total > 43200 or dur_total == 0:
                # if dur_total > 43200:
                    continue
                minute = dur_total / 60 + 1
                # 播放人数
                result[(ver, comepub)][1] += 1
                result[("all", comepub)][1] += 1
                result[(ver, "all")][1] += 1
                result[("all", "all")][1] += 1
                # 播放次数
                result[(ver, comepub)][2] += len(durs_lis)
                result[("all", comepub)][2] += len(durs_lis) if is_allver else 0
                result[(ver, "all")][2] += len(durs_lis)
                result[("all", "all")][2] += len(durs_lis) if is_allver else 0
                # 播放时长
                result[(ver, comepub)][4] += dur_total
                result[("all", comepub)][4] += dur_total if is_allver else 0
                result[(ver, "all")][4] += dur_total
                result[("all", "all")][4] += dur_total if is_allver else 0

                if minute <= 10 and minute >= 1:
                    result[(ver, comepub)][3].setdefault("uv_lte%dmin" % minute, 0)
                    result[("all", comepub)][3].setdefault("uv_lte%dmin" % minute, 0)
                    result[(ver, "all")][3].setdefault("uv_lte%dmin" % minute, 0)
                    result[("all", "all")][3].setdefault("uv_lte%dmin" % minute, 0)
                    result[(ver, comepub)][3]["uv_lte%dmin" % minute] += 1
                    result[("all", comepub)][3]["uv_lte%dmin" % minute] += 1 if is_allver else 0
                    result[(ver, "all")][3]["uv_lte%dmin" % minute] += 1
                    result[("all", "all")][3]["uv_lte%dmin" % minute] += 1 if is_allver else 0
                elif minute > 10:
                    result[(ver, comepub)][3].setdefault("uv_gt10min", 0)
                    result[("all", comepub)][3].setdefault("uv_gt10min", 0)
                    result[(ver, "all")][3].setdefault("uv_gt10min", 0)
                    result[("all", "all")][3].setdefault("uv_gt10min", 0)
                    result[(ver, comepub)][3]["uv_gt10min"] += 1
                    result[("all", comepub)][3]["uv_gt10min"] += 1 if is_allver else 0
                    result[(ver, "all")][3]["uv_gt10min"] += 1
                    result[("all", "all")][3]["uv_gt10min"] += 1 if is_allver else 0
                is_allver = False
            return True
        except:
            import traceback
            print(traceback.print_exc())
            return False

    def transformresult(self, analysisresult, *args, **kwargs):
        result = analysisresult.result
        analysisresult.transformresult = result

