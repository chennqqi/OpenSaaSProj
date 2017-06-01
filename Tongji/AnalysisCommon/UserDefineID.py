# -*- coding: utf-8 -*-
import __init__
from SaaSCommon.JHOpen import JHOpen
import json
from SaaSConfig.config import get_file_path
import time


class UserDefineID(object):

    # opentype 为 in、end、action、page

    def __init__(self):
        self.uvfile_path= None
        self.daily_paths = None

    def setUVFilePath(self, path):
        self.uvfile_path = path

    def setAllDailyPath(self, pathLis):
        self.daily_paths = pathLis

    def getData(self, datatype, num):
        curDay = time.strftime("%Y%m%d", time.localtime(time.time() - 86400 * num))
        paths = self.daily_paths if self.daily_paths else get_file_path(datatype=datatype,
                                                                        yyyymmdd=curDay,
                                                                        hhmm="2359",
                                                                        last=1440)
        result = {}
        for path in paths:
            for line in JHOpen().readLines(path):
                if not line:
                    continue
                try:
                    data = json.loads(line)
                    optype = data["jhd_opType"].strip().replace(" ", "_")
                    if optype not in set(["action", "page"]):
                        continue
                    eventid = data["jhd_eventId"].strip().replace(" ", "_")
                    result.setdefault(optype, set()).add(eventid)
                except:
                    import traceback
                    print(traceback.print_exc())
                    print(line)
        return result
