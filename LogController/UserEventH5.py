# -*- coding: utf-8 -*-
from SaaSMode.UserEventBuilderH5 import UserEventBuilderH5
from UserEvent import UserEvent
from SaaSTools.tools import find_value_fromdict_singlekey
from SaaSCommon.JHOpen import JHOpen
from TransformH5toApp import TransformH5toApp
import json


class UserEventH5(UserEvent):

    def __init__(self, datatype, yyyy_mm_dd, hhmm="2359", last=1440):
        super(UserEventH5, self).__init__(datatype, yyyy_mm_dd, hhmm, last)
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


    def setPaths(self, paths):
        self.paths = paths
        # self.userDict = BigDict()

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


    def parse(self, data, eventEntities, mode=UserEventBuilderH5):
        try:
            eventEntity = super(UserEventH5, self).parse(data, eventEntities, mode=UserEventBuilderH5)
            if not eventEntity:
                return
            device = find_value_fromdict_singlekey(data, self.comparison_table["device"])
            browser = find_value_fromdict_singlekey(data, self.comparison_table["browser"])
            system = find_value_fromdict_singlekey(data, self.comparison_table["system"])
            support = find_value_fromdict_singlekey(data, self.comparison_table["support"])
            eventEntity.setSupport(support)
            eventEntity.setDevice(device)
            eventEntity.setBrowser(browser)
            eventEntity.setSystem(system)
            return eventEntity
        except:
            import traceback
            print(traceback.print_exc())


    # def dataCollect(self):
    #     eventEntities = []
    #     for path in self.paths:
    #         for line in self.pipeline(path):
    #             if not line:
    #                 continue
    #             line = line[0]
    #             data = json.loads(line)
    #             self.parse(data, data)
    #     return eventEntities

