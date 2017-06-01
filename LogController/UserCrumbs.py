# -*- coding: utf-8 -*-
from LogController import LogControler
from SaaSMode.UserCrumbsBuilder import UserCrumbsBuilder
from SaaSTools.tools import find_value_fromdict_singlekey
from SaaSConfig.config import get_file_path
from SaaSCommon.JHOpen import JHOpen
import itertools
import json


class UserCrumbs(LogControler):

    def __init__(self, datatype, yyyy_mm_dd, hhmm="2359", last=1440):
        self.datatype = datatype
        self.yyyy_mm_dd = yyyy_mm_dd
        self.yyyymmdd = yyyy_mm_dd.replace("-", "")
        self.paths = get_file_path(datatype=self.datatype, yyyymmdd=self.yyyymmdd, hhmm=hhmm, last=last)
        self.userDict = {}
        self.singlekey = ["jhd_datatype", "jhd_userkey", "jhd_pushid"]
        self.appendkey = ["jhd_pb", "jhd_vr", "jhd_os", "jhd_netType", "jhd_ua", "jhd_ip"]
        self.countkey = {"item_count": ["jhd_eventId", "jhd_opType", "jhd_pageName"]}
        self.eventtype = {"jhd_eventId": "action", "jhd_pageName": "page"}
        self.addkey = {"item_add": ["jhd_interval"]}
        self.statkeys = list(
            itertools.chain(self.singlekey, self.appendkey, self.countkey["item_count"], self.addkey["item_add"]))
        self.uvkeys = list(
            itertools.chain(self.singlekey, self.appendkey, \
                            self.countkey.keys(), \
                            self.eventtype.keys(), \
                            self.addkey.keys()))

    def setPaths(self, paths):
        self.paths = paths

    def pipeline(self, path):
        for line in JHOpen().readLines(path):
            if not line:
                continue
            data = json.loads(line)
            yield [data]

    def mapaction(self, data):
        try:
            datatype = find_value_fromdict_singlekey(data, "jhd_datatype")
            if datatype in ["biqu", "BIQU_ANDROID", "biqu_all"]:
                eventid = find_value_fromdict_singlekey(data, "jhd_eventId")
                usermap = find_value_fromdict_singlekey(data, "jhd_map")
                # 搜索区分单程、往返机票
                if eventid == "ac36":
                    # et: 返程时间
                    et = usermap.get("et", None) if isinstance(usermap, dict) else None
                    # print(et, type(usermap), usermap)
                    if et:
                        for acid in ["ac36", "jhf_ac36_et1"]:
                            data["jhd_eventId"] = acid
                            yield data
                    else:
                        for acid in ["ac36", "jhf_ac36_et0"]:
                            data["jhd_eventId"] = acid
                            yield data
                # 预订区分单程、往返机票
                elif eventid == "ac49":
                    try:
                        wf = int(usermap["wf"])
                    except:
                        wf = -1
                    if wf == 1:
                        for acid in ["ac49", "jhf_ac49_wf1"]:
                            data["jhd_eventId"] = acid
                            yield data
                    else:
                        for acid in ["ac49", "jhf_ac49_wf0"]:
                            data["jhd_eventId"] = acid
                            yield data
                # 付款区分单程、往返机票
                elif eventid == "ac53":
                    fst = usermap.get("fst", None) if isinstance(usermap, dict) else None
                    if fst:
                        for acid in ["ac53", "jhf_ac53_fst1"]:
                            data["jhd_eventId"] = acid
                            yield data
                    else:
                        for acid in ["ac53", "jhf_ac53_fst0"]:
                            data["jhd_eventId"] = acid
                            yield data
                else:
                    yield data
            else:
                yield data
        except:
            import traceback
            print(traceback.print_exc())

    def parse(self, data, userDict, mode=UserCrumbsBuilder):
        tmp = []
        for item in self.mapaction(data):
            tmp.append(self.parse_item(item, userDict, mode))
        return tmp

    def parse_item(self, data, userDict, mode=UserCrumbsBuilder):
        try:
            # singlekey
            datatype = find_value_fromdict_singlekey(data, "jhd_datatype")
            userkey = find_value_fromdict_singlekey(data, "jhd_userkey")
            userkey = userkey.strip()
            pushid = find_value_fromdict_singlekey(data, "jhd_pushid")
            # appendkey
            os = find_value_fromdict_singlekey(data, "jhd_os")
            pub = find_value_fromdict_singlekey(data, "jhd_pb")
            net = find_value_fromdict_singlekey(data, "jhd_netType")
            vr = find_value_fromdict_singlekey(data, "jhd_vr")
            ua = find_value_fromdict_singlekey(data, "jhd_ua")
            ip = find_value_fromdict_singlekey(data, "jhd_ip")
            # countkey
            opa = find_value_fromdict_singlekey(data, "jhd_opType")
            eventid = find_value_fromdict_singlekey(data, "jhd_eventId")
            eventid = opa if opa in set(["in", "end"]) else eventid
            pagename = find_value_fromdict_singlekey(data, "jhd_pageName")
            # addkey
            intervalStr = find_value_fromdict_singlekey(data, "jhd_interval")
            try:
                interval = float(intervalStr)
            except:
                interval = None
            # other
            usermap = find_value_fromdict_singlekey(data, "jhd_map")
            if usermap == "" or usermap == "null" or (not usermap):
                usermap = -1
            opa_tm = find_value_fromdict_singlekey(data, "jhd_opTime")
            opatm = opa_tm.replace("+", "").replace("-", "").replace("-", "")
            hh_mm_ss = opa_tm.split("+")[1] if "+" in opa_tm else ":".join([opa_tm[8:10], opa_tm[10:12], opa_tm[12:14]])
            userData = mode() if userkey not in userDict else userDict[userkey]
            userData.setDatatype(datatype)
            userData.setUserkey(userkey)
            userData.setPushId(pushid)
            userData.setTM(self.yyyy_mm_dd)
            userData.setOS(os)
            userData.setPub(pub)
            userData.setNet(net)
            userData.setVr(vr)
            userData.setUA(ua)
            userData.setIP(ip)
            userData.setItemCount(eventid, opa, hh_mm_ss, usermap, interval=interval)
            userData.setItemCount(pagename, opa, hh_mm_ss, usermap, interval=interval, isadd=False)
            userData.setLastOpaTime(opatm)
            userDict.setdefault(userkey, userData)
            return userData
        except:
            import traceback
            print(traceback.print_exc())

    def createUserCrumbs(self):
        userDict = {}
        for path in self.paths:
            for line in self.pipeline(path):
                if not line:
                    continue
                try:
                    data = line[0]
                    self.parse(data, userDict)
                except:
                    import traceback
                    print(traceback.print_exc())

        tmp = {}
        [tmp.setdefault(key, userDict[key].builder()) for key in userDict]
        return tmp

    def dataCollect(self, *args, **kwargs):
        userDict = {}
        for path in self.paths:
            for line in self.pipeline(path):
                if not line:
                    continue
                try:
                    data = line[0]
                    self.parse(data, userDict, UserCrumbsBuilder)
                except:
                    import traceback
                    print(traceback.print_exc())

        tmp = {}
        [tmp.setdefault(key, userDict[key].builder()) for key in userDict]
        return tmp

    def mergeUserCrumbs(self, _old, _new=None):
        ucb = UserCrumbsBuilder()
        ucb.mergeUserCrumbs(_old, _new)
        return _old

    def formatList(self, data):
        ucb = UserCrumbsBuilder()
        ucb.formatList(data)
        return data

if __name__ == "__main__":
    tester = UserCrumbs("test", "2016-08-15")
    tester.setPaths(["c:/crumbstest.txt"])
    userDict = {}
    line = '''{"jhd_auth": "on", "jhd_sdk_type": "android", "jhd_ua": "vie-al10", "jhd_map": {"og": "BJS", "et": "2017-06-03", "dt": "SIA", "st": "2017-05-19"}, "jhd_loc": "null", "jhd_pushid": "4921FDC2CA371EE0769A2EA7B359A647D949C962", "jhd_opTime": "20161017234501", "jhd_eventId": "ac36", "jhd_ip": "114.242.248.167", "jhd_pb": "zhihy", "jhd_userkey": "2b379859-53dd-3b71-b8ae-771941d832700", "jhd_os": "android_6.0", "jhd_opType": "action", "jhd_sdk_version": "1.0.1", "jhd_netType": "4g", "jhd_vr": "1.1.2", "jhd_ts": "1476719101999", "jhd_interval": "0", "jhd_datatype": "BIQU_ANDROID"}'''
    data = tester.parse(json.loads(line), userDict)
    print(data)
    # from SaaSStore.StoreMongo import StoreMongo
    # store = StoreMongo()
    # store.write_uvfile(data, "feeling", "uvfile", "2016-08-22", tester)
    # print(json.dumps(data))

    # for key in data:
    #     print(key, data[key])

