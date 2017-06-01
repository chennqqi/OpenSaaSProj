# -*- coding: utf-8 -*-
from Tongji.AnalysisMap.AnalysisMap import AnalysisMap
from SaaSCommon.JHOpen import JHOpen
from SaaSConfig.config import get_uvfile_path
import time


class AnalysisOnlineDay(AnalysisMap):

    def __init__(self):
        self.finished = False

    # 在线天数(新增)
    def rules(self, analysisresult, data, num, *args, **kwargs):
        num += 7
        result = analysisresult.result
        datatype = kwargs["datatype"]
        curDay = time.strftime("%Y%m%d", time.localtime(time.time() - 86400 * num))
        try:
            tmp = {}
            newuserData = self.newcomers(datatype, num)
            newusers = newuserData.get(("all", "all"), set())
            # 计算每一个新增用户的活跃天数
            for i in range(num-7, num):
                j = num - i
                activeUser = self.activeUser(datatype, j)
                for uid in newusers:
                    tmp.setdefault(uid, [0])
                    if uid in activeUser:
                        tmp.setdefault(uid, [0])[0] += 1
            # 计算新增活跃天数，分版本、分渠道
            uvfile_path = get_uvfile_path(num, datatype, iszip=True)
            for line in JHOpen().readLines(uvfile_path):
                if not line:
                    continue
                try:
                    items = line.split("\t")
                    uid = items[0].strip()
                    isnewcomer = True if curDay == items[8][:8] else False
                    if not isnewcomer:
                        continue
                    curpub = items[3].strip()
                    comepub = items[2].strip() if items[2].strip() != "#" else curpub.split("#")[0]
                    vers = items[5].strip()
                    is_allver = True
                    for ver in vers.split("#"):
                        days = tmp.get(uid, [0])[0]
                        result.setdefault((ver, comepub), [{}, 0])[0].setdefault(days, set()).add(uid)
                        result.setdefault((ver, "all"), [{}, 0])[0].setdefault(days, set()).add(uid)
                        result.setdefault(("all", comepub), [{}, 0])[0].setdefault(days, set()).add(uid)
                        result.setdefault(("all", "all"), [{}, 0])[0].setdefault(days, set()).add(uid)
                        # 新增
                        result.setdefault((ver, comepub), [{}, 0])[1] += 1
                        result.setdefault((ver, "all"), [{}, 0])[1] += 1
                        result.setdefault(("all", comepub), [{}, 0])[1] += 1 if is_allver else 0
                        result.setdefault(("all", "all"), [{}, 0])[1] += 1 if is_allver else 0
                        is_allver = False
                except:
                    import traceback
                    print(traceback.print_exc(), line)
            self.finished = True
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
        result = {}
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

