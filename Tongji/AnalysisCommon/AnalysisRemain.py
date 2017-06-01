# -*- coding: utf-8 -*-
from Tongji.AnalysisMap.AnalysisMap import AnalysisMap
from SaaSCommon.JHOpen import JHOpen
from SaaSConfig.config import get_uvfile_path
import time


class AnalysisRemain(AnalysisMap):

    def __init__(self):
        self.finished = False

    # def rules(self, analysisresult, datatype, num, *args, **kwargs):
    def rules(self, analysisresult, data, num, *args, **kwargs):
        result = analysisresult.result
        datatype = kwargs["datatype"]
        try:
            remain_last = [0, 1, 3, 7, 15, 30, 90]
            # 获取活跃用户数
            activeUser = self.activeUser(datatype, num)
            for _num in remain_last:
                try:
                    data = self.newcomers(datatype, num + _num)
                except:
                    import traceback
                    print(traceback.print_exc())
                    continue
                for key in data:
                    result.setdefault(key, {})[_num] = len(data[key] & activeUser) + result.setdefault(key, {}).get(_num, 0)
            self.finished = True
            return True
        except:
            import traceback
            print(traceback.print_exc())
            self.finished = True
            return False

    def transformresult(self, analysisresult, *args, **kwargs):
        result = analysisresult.result
        analysisresult.transformresult = result

    # 获取活跃用户
    def activeUser(self, datatype, num):
        users = set()
        uvfile_path = get_uvfile_path(num, datatype, iszip=True)
        for line in JHOpen().readLines(uvfile_path):
            if not line:
                continue
            try:
                items = line.split("\t")
                uid = items[0].strip()
                isactive = True if items[1].strip() == "1" else False
                if not isactive:
                    continue
                users.add(uid)
            except:
                import traceback
                print(traceback.print_exc())
        return users


    # 获取新增(分版本、渠道)
    def newcomers(self, datatype, num):
        uvfile_path = get_uvfile_path(num, datatype, iszip=True)
        curDay = time.strftime("%Y%m%d", time.localtime(time.time() - 86400 * num))
        # 此处必须赋初值
        result = {("all", "all"): set()}
        for line in JHOpen().readLines(uvfile_path):
            if not line:
                continue
            try:
                items = line.split("\t")
                uid = items[0].strip()
                isnewcomer = True if curDay == items[8][:8] else False
                if not isnewcomer:
                    continue
                isactive = True if items[1].strip() == "1" else False
                if not isactive:
                    continue
                curpub = items[3].strip()
                comepub = items[2].strip() if items[2].strip() != "#" else curpub.split("#")[0]
                vers = items[5].strip()
                for ver in vers.split("#"):
                    result.setdefault((ver, comepub), set()).add(uid) if isnewcomer else None
                    result.setdefault(("all", comepub), set()).add(uid) if isnewcomer else None
                    result.setdefault((ver, "all"), set()).add(uid) if isnewcomer else None
                    result.setdefault(("all", "all"), set()).add(uid) if isnewcomer else None
            except:
                import traceback
                print(traceback.print_exc())
        return result

