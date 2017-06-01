# -*- coding: utf-8 -*-
from BasicMode import BasicMode


class UserProfileBuilder(BasicMode):
    def __init__(self):
        self._user = {}
        self._user.setdefault("activelife", [0])

    def setUid(self, uid):
        if uid is not None and uid.strip() != "":
            self._user.update({"_id": uid.strip()})
        return self

    def setFirstLoginTime(self, firstLoginTime):
        if firstLoginTime is not None and firstLoginTime.strip() != "":
            self._user.update({"firstLoginTime": firstLoginTime})
        return self

    def setPub(self, pub):
        if pub is not None and pub.strip() != "":
            self._user.setdefault("comepub", pub)
            self._user.update({"curpub": pub})
        return self

    def setPlat(self, plat):
        if plat is not None and plat.strip() != "":
            self._user.update({"plat": plat})
        return self

    def setOS(self, user_os):
        if user_os is not None and user_os.strip() != "":
            self._user.update({"os": user_os})
        return self

    def setGender(self, gender):
        if gender is not None and gender.strip() != "":
            self._user.update({"gender": gender})
        return self

    def setLastLoginTime(self, lastLoginTime):
        if lastLoginTime is not None and lastLoginTime.strip() != "":
            if lastLoginTime > self._user.get("lastLoginTime", "19700101000000"):
                self._user.update({"lastLoginTime": lastLoginTime})
        return self

    def setVer(self, ver):
        if ver is not None and ver.strip() != "":
            self._user.update({"ver": ver})
        return self
    #
    # def setIP(self, ip):
    #     if ip is not None and (isinstance(ip, str) or isinstance(ip, unicode)) and ip.strip() != "":
    #         self._user.setdefault("ip", set()).add(ip)
    #     return self

    def setPushid(self, pushid):
        if pushid is not None and pushid.strip() != "":
            self._user.update({"pushid": pushid})
        return self

    def setUA(self, ua):
        if ua is not None and ua.strip() != "":
            self._user.update({"ua": ua})
        return self

    def setProperties(self, properties):
        if type(properties) != dict : return self
        self._user["properties"] = properties
        return self

    def build(self):
        return self._user

    def formatList(self, data=None, keys = ["ip"]):
        if data is None:
            data = self._user
        for key in keys:
            if key in data:
                data[key] = list(data[key])

    def mergeUserProfile(self, _old, _new = None):
        _new = self._user if _new is None else _new
        if _old.get("firstLoginTime", None) is not None:
            _new["firstLoginTime"] = min([_old["firstLoginTime"], _new["firstLoginTime"]]) \
                if _new.get("firstLoginTime", None) is not None else _old["firstLoginTime"]
        if _old.get("lastLoginTime", None) is not None:
            _new["lastLoginTime"] = max([_old["lastLoginTime"], _new["lastLoginTime"]]) \
                if _new.get("lastLoginTime", None) is not None else _old["lastLoginTime"]
        _new["properties"] = dict(_old["properties"], **_new["properties"])
        return dict(_old, **_new)

    def builder(self):
        return self.build()

    def merge(self, _old, _new = None):
        return self.mergeUserProfile(_old, _new)