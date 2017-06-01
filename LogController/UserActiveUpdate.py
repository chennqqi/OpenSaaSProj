# -*- coding: utf-8 -*-
from LogController import LogControler
from SaaSMode.UserActiveUpdateBuilder import UserActiveUpdateBuilder
from SaaSTools.tools import find_value_fromdict_singlekey
from SaaSConfig.config import get_file_path
from SaaSCommon.JHOpen import JHOpen
import itertools
import json


class UserActiveUpdate(LogControler):

    def __init__(self, datatype, yyyy_mm_dd, hhmm="2359", last=1440):
        self.datatype = datatype
        self.yyyy_mm_dd = yyyy_mm_dd
        self.yyyymmdd = yyyy_mm_dd.replace("-", "")
        self.paths = get_file_path(datatype=self.datatype, yyyymmdd=self.yyyymmdd, hhmm=hhmm, last=last)
        self.userDict = {}

    def setPaths(self, paths):
        self.paths = paths

    def pipeline(self, path):
        for line in JHOpen().readLines(path):
            if not line:
                continue
            data = json.loads(line)
            yield [data]

    def parse(self, data, userDict, mode=UserActiveUpdateBuilder):
        userData = []
        try:
            userkey = find_value_fromdict_singlekey(data, "jhd_userkey")
            user_active = mode() if userkey not in self.userDict else self.userDict[userkey]
            user_active.setUserkey(userkey)
            self.userDict.setdefault(userkey, user_active)
            return user_active
        except:
            import traceback
            print(traceback.print_exc())

    def dataCollect(self, *args, **kwargs):
        for path in self.paths:
            for line in self.pipeline(path):
                if not line:
                    continue
                try:
                    data = line[0]
                    self.parse(data, self.userDict, UserActiveUpdateBuilder)
                except:
                    import traceback
                    print(traceback.print_exc())
        tmp = {}
        [tmp.setdefault(key, self.userDict[key].builder()) for key in self.userDict]
        return tmp

    def mergeUserCrumbs(self, _old, _new=None):
        pass

    def formatList(self, data):
        pass

if __name__ == "__main__":
    pass

