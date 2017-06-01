# -*- coding: utf-8 -*-
from Tongji.AnalysisMap.AnalysisMap import AnalysisMap
from SaaSCommon.JHOpen import JHOpen
from SaaSConfig.config import get_uvfile_path
from SaaSTools.tools import getWeekDays
import time


class AnalysisOverallWeek(AnalysisMap):

    def __init__(self):
        self.finished = False

    def rules(self, analysisresult, data, *args, **kwargs):
        datatype = kwargs["datatype"]
        num = kwargs["num"]
        result = analysisresult.result
        try:
            days = getWeekDays(num)
            for _num in days.values():
                self.activeUserDetail(datatype, _num, result)
            self.finished = False
            return True
        except:
            import traceback
            print(traceback.print_exc())
            self.finished = False
            return False

    def transformresult(self, analysisresult, *args, **kwargs):
        result = analysisresult.result
        analysisresult.transformresult = result

    # 获取活跃用户(分版本、渠道)
    def activeUserDetail(self, datatype, num, data={}):
        data = data
        uvfile_path = get_uvfile_path(num, datatype, iszip=True)
        log_count = 0
        err_count = 0
        for line in JHOpen().readLines(uvfile_path):
            if not line:
                continue
            log_count += 1
            try:
                items = line.split("\t")
                uid = items[0].strip()
                isactive = True if items[1].strip() == "1" else False
                if not isactive:
                    continue
                comepub = items[2].strip()
                vers = items[5].strip()
                for ver in vers.split("#"):
                    data.setdefault((ver, comepub), set())
                    data.setdefault((ver, "all"), set())
                    data.setdefault(("all", comepub), set())
                    data.setdefault(("all", "all"), set())
                    data[(ver, comepub)].add(uid)
                    data[(ver, "all")].add(uid)
                    data[("all", comepub)].add(uid)
                    data[("all", "all")].add(uid)
            except:
                import traceback
                print(traceback.print_exc())
                err_count += 1
        return data

