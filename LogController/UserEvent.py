# -*- coding: utf-8 -*-
from LogController import LogControler
from SaaSMode.UserEventBuilder import UserEventBuilder
from SaaSTools.tools import find_value_fromdict_singlekey
from SaaSCommon.JHOpen import JHOpen
from SaaSConfig.config import get_file_path
import json
import time

# Event Mode
#                 "jhd_opType",
#                 "jhd_eventId",
#                 # who
#                 "jhd_userkey",
#                 "jhd_pushid",
#                 # when
#                 "jhd_ts",
#                 # where
#                 "jhd_ip",
#                 # how
#                 "jhd_sdk_version",
#                 "jhd_pb",
#                 "jhd_os",
#                 "jhd_netType",
#                 "jhd_ua",
#                 "jhd_vr",
#                 # what
#                 "jhd_map"]


class UserEvent(LogControler):

    def __init__(self, datatype, yyyy_mm_dd, hhmm="2359", last=1440):
        self.datatype = datatype
        self.yyyy_mm_dd = yyyy_mm_dd
        self.yyyymmdd = yyyy_mm_dd.replace("-", "")
        self.paths = get_file_path(datatype=self.datatype, yyyymmdd=self.yyyymmdd, hhmm=hhmm, last=last)
        self.userDict = {}

    def setPaths(self, paths):
        self.paths = paths
        # self.userDict = BigDict()

    def pipeline(self, path):
        for line in JHOpen().readLines(path):
            if not line:
                continue
            data = json.loads(line)
            yield [data]

    def parse(self, data, eventEntities, mode=UserEventBuilder):
        try:
            eventEntity = mode()
            optype = find_value_fromdict_singlekey(data, "jhd_opType")
            eventid = find_value_fromdict_singlekey(data, "jhd_eventId")
            userkey = find_value_fromdict_singlekey(data, "jhd_userkey")
            userkey = userkey.strip() if userkey else userkey
            pushid = find_value_fromdict_singlekey(data, "jhd_pushid")
            pushid = pushid.strip() if pushid else pushid
            ts = find_value_fromdict_singlekey(data, "jhd_ts")
            jhd_opTime = find_value_fromdict_singlekey(data, "jhd_opTime")
            if jhd_opTime is None and ts:
                jhd_opTime = time.strftime("%Y%m%d%H%M%S", time.localtime(ts))
            if ts is None and jhd_opTime:
                ts = time.mktime(time.strptime(jhd_opTime, "%Y%m%d%H%M%S")) * 1000
            ip = find_value_fromdict_singlekey(data, "jhd_ip")
            sdk_version = find_value_fromdict_singlekey(data, "jhd_sdk_version")
            pub = find_value_fromdict_singlekey(data, "jhd_pb")
            os = find_value_fromdict_singlekey(data, "jhd_os")
            net = find_value_fromdict_singlekey(data, "jhd_netType")
            ua = find_value_fromdict_singlekey(data, "jhd_ua")
            vr = find_value_fromdict_singlekey(data, "jhd_vr")
            jhmap = find_value_fromdict_singlekey(data, "jhd_map")
            # Event
            eventEntity.setOpType(optype)
            eventEntity.setEventId(eventid)
            # who
            eventEntity.setUid(userkey)
            eventEntity.setPushid(pushid)
            # when
            eventEntity.setTS(ts)
            # where
            eventEntity.setIP(ip)
            # how
            eventEntity.setPub(pub)
            eventEntity.setOS(os)
            eventEntity.setNet(net)
            eventEntity.setUA(ua)
            eventEntity.setVR(vr)
            eventEntity.setMap(jhmap)
            # what
            eventEntity.setSdkVr(sdk_version)
            eventEntities.append(eventEntity.build())
            return eventEntity
        except:
            import traceback
            print(traceback.print_exc())


    def dataCollect(self, *args, **kwargs):
        eventEntities = []
        for path in self.paths:
            for line in self.pipeline(path):
                if not line:
                    continue
                try:
                    data = line[0]
                    self.parse(data, eventEntities)
                except:
                    import traceback
                    print(traceback.print_exc())
        return eventEntities

