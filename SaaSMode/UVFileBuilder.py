# -*- coding: utf-8 -*-
from SaaSTools.IPtoLoc import iploc_demo
from BasicMode import BasicMode


class UVFileBuilder(BasicMode):

    def __init__(self, appkey):
        self.appkey = appkey
        self._item = {}
        self._item.setdefault("in", [0])
        self._item.setdefault("end", [])
        self._item.setdefault("action", {})
        self._item.setdefault("page", {})
        self._item.setdefault("isactive", 0)
        # self.active_opa = ["in", "end", "openpush", "action", "page"]

    def setUid(self, uid):
        self._item["userId"] = uid.strip()
        return self

    def setPlat(self, plat):
        self._item.setdefault("plat", plat)
        return self

    def setUA(self, ua):
        self._item.setdefault("ua", ua)
        return self

    def setNet(self, net):
        self._item.setdefault("net", set()).add(net)
        # self._item.setdefault("net", []).append(net) if net not in self._item.get("net", []) else None
        return self

    def setCurPub(self, pub):
        self._item.setdefault("curpubs", set()).add(pub)
        # self._item.setdefault("curpubs", []).append(pub) \
        #     if pub not in self._item.get("curpubs", []) else None
        return self

    def setComePub(self, pub):
        self._item.setdefault("comepub", pub)
        return self

    def setFirstLoginTime(self, firstLoginTime):
        self._item.setdefault("firstLoginTime", firstLoginTime)
        return self

    def setLastLoginTime(self, lastLoginTime):
        self._item.setdefault("lastLoginTime", lastLoginTime)
        return self

    def setVer(self, ver):
        self._item.setdefault("vers", set()).add(ver)
        # self._item.setdefault("vers", []).append(ver) \
        #     if ver not in self._item.get("vers", []) else None
        return self

    def setPushid(self, pushid):
        if pushid:
            self._item["pushid"] = pushid
        return self

    def setLoc(self, ip):
        loc = iploc_demo.getLoc(ip)
        self._item.setdefault("locs", set()).add(loc)
        # self._item.setdefault("locs", []).append(loc) \
        #     if loc not in self._item.get("locs", []) else None
        return self

    # 需要初始化
    def setIn(self):
        self._item.setdefault("in", [0])[0] += 1
        self._item.update({"isactive": 1})
        return self

    # 需要初始化
    def setEnd(self, dur):
        if dur:
            self._item.setdefault("end", []).append(dur)
        else:
            self._item.setdefault("end", []).append(0)
        self._item.update({"isactive": 1})
        return self

    # 需要初始化
    def setAction(self, action):
        self._item.setdefault("action", {}).setdefault(action, [0])[0] += 1
        if ("action" != "pushaccess") and not ((self.appkey == "caiyu_ad" or self.appkey == "caiyu_ios_free") and action == "ac57"):
            self._item.update({"isactive": 1})
        return self

    # 需要初始化
    def setPage(self, pagename):
        self._item.setdefault("page", {}).setdefault(pagename, [0])[0] += 1
        self._item.update({"isactive": 1})
        return self

    def build(self):
        return self._item

    def builder(self):
        return self.build()

    def merge(self, _old, _new = None):
        pass

    # def mergeUVFile(self, _old, _new):
    #     _new = self._user if _new is None else _new
    #     _new["net"] = _new.get("net", set())|_old.get("net", set())
    #     _new["curpubs"] = _new.get("curpubs", set())| _old.get("curpubs", set())
    #     _new["vers"] = _new.get("vers", set()) | _old.get("vers", set())
    #     _new["locs"] = _new.get("locs", set()) | _old.get("locs", set())






