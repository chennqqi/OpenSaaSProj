# -*- coding: utf-8 -*-
from LogController import LogControler
from SaaSMode.UserProfileBuilder import UserProfileBuilder
from SaaSConfig.config import get_file_path
from SaaSTools.tools import find_value_fromdict_singlekey
from SaaSCommon.JHOpen import JHOpen
import json
import time


'''
### 用户属性数据 ###
Format:
{
"_id" : "9899330410654939c:73:91:0c:a3:76com.easyen.channelmobileteacher",
"pub" : "23",
"plat" : "android",
"firstLoginTime" : "20160804212502",
"lastLoginTime" : "20160804212606",
"properties" : {  }
}
'''


class UserProfile(LogControler):

    def __init__(self, datatype, yyyy_mm_dd, hhmm="2359", last = 1):
        self.datatype = datatype
        self.yyyy_mm_dd = yyyy_mm_dd
        self.yyyymmdd = yyyy_mm_dd.replace("-", "")
        self.hhmm = hhmm
        self.paths = get_file_path(datatype=datatype, yyyymmdd=self.yyyymmdd, hhmm=hhmm, last=last)
        self.paths.sort()
        self.data = {}

    def setPaths(self, paths):
        self.paths = paths

    def pipeline(self, path):
        for line in JHOpen().readLines(path):
            if not line:
                continue
            data = json.loads(line)
            yield [data]

    def parse(self, dataDict, userDict, mode=UserProfileBuilder):
        try:
            userProfile = mode()
            userkey = find_value_fromdict_singlekey(dataDict, "jhd_userkey")
            userkey = userkey.strip()
            ver = find_value_fromdict_singlekey(dataDict, "jhd_vr")
            userProfile.setUid(userkey)
            opTime = find_value_fromdict_singlekey(dataDict, "jhd_opTime").replace("-", "").replace(":", "")
            # 以用户实际操作时间作为 LastLoginTime
            if len(opTime) == 14:
                # opTime = "".join([self.yyyymmdd, self.hhmm])
                try:
                    opTime = time.strftime("%Y%m%d%H%M%S", time.strptime("".join([self.yyyymmdd, opTime[8:]]), "%Y%m%d%H%M%S"))
                except:
                    opTime = "".join([self.yyyymmdd, self.hhmm])
                    opTime = time.strftime("%Y%m%d%H%M%S", time.strptime(opTime, "%Y%m%d%H%M%S"))
            else:
                opTime = "".join([self.yyyymmdd, "000001"])
            userProfile.setPub(find_value_fromdict_singlekey(dataDict, "jhd_pb"))
            # userProfile.setPlat(find_value_fromdict_singlekey(dataDict, "jhd_os").split("_")[0])
            userProfile.setPlat(find_value_fromdict_singlekey(dataDict, "jhd_os").split("_")[0] if find_value_fromdict_singlekey(dataDict, "jhd_os") else "#")
            userProfile.setOS(find_value_fromdict_singlekey(dataDict, "jhd_os"))
            userProfile.setPushid(find_value_fromdict_singlekey(dataDict, "jhd_pushid"))
            userProfile.setUA(find_value_fromdict_singlekey(dataDict, "jhd_ua"))
            userProfile.setFirstLoginTime(opTime)
            userProfile.setLastLoginTime(opTime)
            userProfile.setVer(ver)
            userProfile.setProperties(dataDict.get("jhd_properties", {}))
            if (userDict.has_key(userkey)):
                userDict[userkey] = \
                    self.mergeUserProfile(userProfile.build(), userDict[userkey])
            else:
                userDict[userkey] = userProfile.build()
        except:
            import traceback
            print(traceback.print_exc())
        return userProfile

    def dataCollect(self, *args, **kwargs):
        userDict = {}
        for path in self.paths:
            for line in self.pipeline(path):
                if not line:
                    continue
                try:
                    line = line[0]
                    if isinstance(line, dict):
                        dataDict = line
                    elif isinstance(line, type("")):
                        dataDict = json.loads(line)
                    else:
                        continue
                    self.parse(dataDict, userDict)
                except:
                    import traceback
                    print(traceback.print_exc())
        return userDict

    def mergeUserProfile(self, _new, _old):
        userProfile = UserProfileBuilder()
        return userProfile.mergeUserProfile(_new, _old)

if __name__ == "__main__":
    tester = UserProfile(datatype="111", yyyy_mm_dd="2016-09-26", hhmm="0012")
    tester.setPaths(["C:/test.txt"])
    print tester.dataCollect()










