# -*- coding: utf-8 -*-

# -*- coding: utf-8 -*-
from SaaSMode.UserCrumbsBuilderH5 import UserCrumbsBuilderH5
from SaaSTools.tools import find_value_fromdict_singlekey
from TransformH5toApp import TransformH5toApp
from SaaSCommon.JHOpen import JHOpen
from UserCrumbs import UserCrumbs

class UserCrumbsH5(UserCrumbs):

    def __init__(self, datatype, yyyy_mm_dd, hhmm="2359", last=1440):
        super(UserCrumbsH5, self).__init__(datatype, yyyy_mm_dd, hhmm, last)

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


    def parse(self, data, userDict, mode=UserCrumbsBuilderH5):
        try:
            for userData in super(UserCrumbsH5, self).parse(data, userDict, mode=UserCrumbsBuilderH5):
                if not userData:
                    return
                device = find_value_fromdict_singlekey(data, self.comparison_table["device"])
                browser = find_value_fromdict_singlekey(data, self.comparison_table["browser"])
                system = find_value_fromdict_singlekey(data, self.comparison_table["system"])
                support = find_value_fromdict_singlekey(data, self.comparison_table["support"])
                userData.setSupport(support)
                userData.setDevice(device)
                userData.setBrowser(browser)
                userData.setSystem(system)
                return userData
        except:
            import traceback
            print(traceback.print_exc())

    def mergeUserCrumbs(self, _old, _new=None):
        ucb = UserCrumbsBuilderH5()
        ucb.mergeUserCrumbs(_old, _new)
        return _old

    def formatList(self, data):
        ucb = UserCrumbsBuilderH5()
        ucb.formatList(data)
        return data


if __name__ == "__main__":
    tester = UserCrumbsH5("test", "2016-08-15")
    tester.setPaths(["C:/test.txt"])
    data = tester.createUserCrumbs()
    print(data)