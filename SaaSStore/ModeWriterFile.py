# -*- coding: utf-8 -*-
from collections import OrderedDict
import json
import time
from ModeWriter import ModeWriter
from SaaSCommon.JHWrite import JHWrite
from SaaSConfig.config import get_uvfile_path
from DBClient.PyMongoClient import PyMongoClient
import json


class UVFileWriter(ModeWriter):

    def __init__(self, mongo_id=1):
        self.mongo_id = mongo_id

    def remove(self, *args, **kwargs):
        pass

    # def write(self, data, appkey, num, *args, **kwargs):
    def write(self, dataDict, appkey, modename, modetools, *args, **kwargs):
        num = kwargs["num"]
        client = PyMongoClient(self.mongo_id)
        uvfile_path = get_uvfile_path(num, appkey)
        cur_day = time.strftime("%Y-%m-%d", time.localtime(time.time()-86400*num))
        uids = dataDict.keys()
        # userProfile = client.find(appkey, "UserProfile", {"_id": {"$in": uids}})
        uvfile = client.findElemIn(appkey, "uvfile", "jhd_userkey", uids, OrderedDict([("tm", cur_day)]), {"_id": False, "jhd_userkey": True, "jhd_loc": True, "firstLoginTime": True, "lastOpaTime": True, "jhd_pb": True})
        for item in uvfile:
            try:
                uid = item["jhd_userkey"]
                comepub = item.get("jhd_pb", ["#"])
                firstLoginTime = item.get("firstLoginTime", "#")
                lastLoginTime = item.get("lastOpaTime", "#")
                data = dataDict[uid]
                data["firstLoginTime"] = firstLoginTime
                data["lastLoginTime"] = lastLoginTime
                data["comepub"] = comepub[0] if comepub else "#"
                locs = item.get("jhd_loc", None)
                if locs:
                    data["locs"] = []
                    for item in locs:
                        if isinstance(item, dict):
                            prov, city = item.get("prov", "#"), item.get("city", "#")
                        else:
                            prov, city = item.split("_")
                        if (prov, city) not in data["locs"]:
                            data["locs"].append((prov, city))
            except:
                import traceback
                print traceback.print_exc()
        for key in dataDict:
            data = dataDict[key]
            uid = key
            pushid = data["pushid"]
            plat = data["plat"]
            ua = data["ua"]
            net = "#".join(list(data["net"]))
            curpub = "#".join(list(data["curpubs"]))
            comepub = data["comepub"]
            firstLoginTime = data["firstLoginTime"]
            lastLoginTime = data["lastLoginTime"]
            ver = "#".join(list(data["vers"]))
            loc = "#".join(map(lambda item: "_".join(item), data["locs"]))
            in_num = data["in"][0]
            dur = "#".join(map(str, data["end"])) if data["end"] else "#"
            actions = {}
            [actions.setdefault(key, data["action"][key][0]) for key in data["action"]]
            actionDict = json.dumps(actions)
            pages = {}
            [pages.setdefault(key, data["page"][key][0]) for key in data["page"]]
            pageDict = json.dumps(pages)
            isactive = data["isactive"]
            line = []
            line.append(uid) # 1
            line.append(isactive)
            line.append(comepub)
            line.append(curpub)
            line.append(plat) # 5
            line.append(ver)
            line.append(ua)
            line.append(net)
            line.append(firstLoginTime)
            line.append(lastLoginTime) # 10
            line.append(loc)
            line.append(in_num)
            line.append(dur)
            line.append(actionDict)
            line.append(pageDict) # 15
            line.append(pushid)  # 16
            JHWrite(uvfile_path, "\t".join(map(str, line)))
        JHWrite.finished(iszip=True)