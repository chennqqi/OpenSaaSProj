# -*- coding: utf-8 -*-
from TransformH5toApp import TransformH5toApp
from SaaSMode.UserProfileBuilderH5 import UserProfileBuilderH5
from UserProfile import UserProfile
from SaaSCommon.JHOpen import JHOpen
from SaaSTools.tools import find_value_fromdict_singlekey


class UserProfileH5(UserProfile):

    def __init__(self, datatype, yyyy_mm_dd, hhmm = "2359", last = 1):
        super(UserProfileH5, self).__init__(datatype, yyyy_mm_dd, hhmm, last)
        self.transform = TransformH5toApp()
        self.comparison_table = {
            "jhd_datatype": "appkey",
            "jhd_userkey": "uid",
            "jhd_ip": "ip",
            "jhd_opType": "type",
            "jhd_vr": "vr",
            "jhd_ts": "ts",
            "jhd_map": "usermap",
            "jhd_pageName": "uri",
            "jhd_eventId": "event",
            "jhd_interval": "value",

            "device": "device",
            "browser": "browser",
            "system": "system",
            "support": "support",
            "ref": "ref",
            "value": "value"
        }
        self.comparison_key = {
            "ac": "action",
            "dur": "end",
            "page": "page"
        }

    def pipeline(self, path):
        for line in JHOpen().readLines(path):
            if not line:
                continue
            try:
                data = self.transform.transform(line)
                yield [data]
            except:
                import traceback
                print(traceback.print_exc())
            yield []

    def parse(self, data, userDict, mode=UserProfileBuilderH5):
        try:
            userProfile = super(UserProfileH5, self).parse(data, userDict, mode=mode)
            if not userProfile:
                return
            device = find_value_fromdict_singlekey(data, self.comparison_table["device"])
            browser = find_value_fromdict_singlekey(data, self.comparison_table["browser"])
            system = find_value_fromdict_singlekey(data, self.comparison_table["system"])
            support = find_value_fromdict_singlekey(data, self.comparison_table["support"])
            userProfile.setSupport(support)
            userProfile.setDevice(device)
            userProfile.setBrowser(browser)
            userProfile.setSystem(system)
            return userProfile
        except:
            import traceback
            print(traceback.print_exc())


if __name__ == "__main__":
    tester = UserProfileH5(datatype="111", yyyy_mm_dd="2016-09-26", hhmm="0012")
    tester.setPaths(["C:/test.txt"])
    print tester.dataCollect()
