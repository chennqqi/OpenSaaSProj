# -*- coding: utf-8 -*-
from LogController import LogControler
from SaaSCommon.JHOpen import JHOpen
import json
from SaaSMode.UVFileBuilder import UVFileBuilder
from SaaSConfig.config import get_file_path
from SaaSCommon.BigDict import BigDict


class UVFile(LogControler):

    def __init__(self, datatype, yyyy_mm_dd, hhmm="0159", last=1440):
        self.datatype = datatype
        self.yyyy_mm_dd = yyyy_mm_dd
        self.yyyymmdd = yyyy_mm_dd.replace("-", "")
        self.paths = get_file_path(datatype=self.datatype, yyyymmdd=self.yyyymmdd, hhmm=hhmm, last=last)
        # self.userDict = {}
        self.userDict = BigDict()

    def setPaths(self, paths):
        self.paths = paths

    # 废弃方法，内部使用
    def getUVFile(self):
        result = {}
        for key in self.userDict:
            data = self.userDict[key].build()
            result.setdefault(key, data)
        return result

    def pipeline(self, path):
        for line in JHOpen().readLines(path):
            if not line:
                continue
            try:
                data = json.loads(line.strip())
                yield [data]
            except:
                import traceback
                print(traceback.print_exc())
            yield []

    # 废弃方法，不能使用
    def createUVFile(self):
        for path in self.paths:
            for line in self.pipline(path):
                if not line:
                    continue
                try:
                    data = line[0]
                    uid = data["jhd_userkey"].strip()
                    pushid = data.get("jhd_pushid", "#")
                    if not pushid:
                        pushid = "#"
                    op_type = data["jhd_opType"]
                    uvfile_item = UVFileBuilder(self.datatype) if uid not in self.userDict else self.userDict[uid]
                    uvfile_item.setUid(data["jhd_userkey"])
                    uvfile_item.setPushid(pushid)
                    uvfile_item.setPlat(data["jhd_os"])
                    uvfile_item.setUA(data["jhd_ua"])
                    uvfile_item.setNet(data["jhd_netType"])
                    uvfile_item.setCurPub(data["jhd_pb"])
                    uvfile_item.setVer(data["jhd_vr"])
                    uvfile_item.setLoc(data["jhd_ip"])
                    # 占位
                    uvfile_item.setComePub("#")
                    uvfile_item.setFirstLoginTime("#")
                    uvfile_item.setLastLoginTime("#")
                    if op_type == "in":
                        uvfile_item.setIn()
                    elif op_type == "end":
                        uvfile_item.setEnd(data["jhd_interval"])
                    elif op_type == "action":
                        uvfile_item.setAction(data["jhd_eventId"])
                    elif op_type == "page":
                        uvfile_item.setPage(data["jhd_eventId"])
                    self.userDict[uid] = uvfile_item
                except:
                    import traceback
                    print(traceback.print_exc())

    def parse(self, data, modedict, mode):
        uid = data["jhd_userkey"].strip()
        pushid = data.get("jhd_pushid", "#")
        if not pushid:
            pushid = "#"
        op_type = data["jhd_opType"]
        uvfile_item = mode(self.datatype) if uid not in modedict else modedict[uid]
        uvfile_item.setUid(data["jhd_userkey"])
        uvfile_item.setPushid(pushid)
        uvfile_item.setPlat(data["jhd_os"])
        uvfile_item.setUA(data["jhd_ua"])
        uvfile_item.setNet(data["jhd_netType"])
        uvfile_item.setCurPub(data["jhd_pb"])
        uvfile_item.setVer(data["jhd_vr"])
        uvfile_item.setLoc(data["jhd_ip"])
        # 占位
        uvfile_item.setComePub("#")
        uvfile_item.setFirstLoginTime("#")
        uvfile_item.setLastLoginTime("#")
        if op_type == "in":
            uvfile_item.setIn()
        elif op_type == "end":
            uvfile_item.setEnd(data["jhd_interval"])
        elif op_type == "action":
            uvfile_item.setAction(data["jhd_eventId"])
        elif op_type == "page":
            uvfile_item.setPage(data["jhd_eventId"])
        modedict[uid] = uvfile_item
        return uvfile_item

    def dataCollect(self, *args, **kwargs):
        for path in self.paths:
            for line in self.pipeline(path):
                if not line:
                    continue
                try:
                    line = line[0]
                    if isinstance(line, dict):
                        dataDict = line
                    elif isinstance(line, type("")) or isinstance(line, type(u"")):
                        dataDict = json.loads(line)
                    else:
                        continue
                    self.parse(dataDict, self.userDict, UVFileBuilder)
                except:
                    import traceback
                    print(traceback.print_exc())
        return self.getUVFile()

