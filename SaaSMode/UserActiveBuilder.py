# -*- coding: utf-8 -*-
from BasicMode import BasicMode

class UserActiveBuilder(BasicMode):

    def __init__(self):
        self._useractive = {}

    def setActive(self, active_lis):
        assert type(active_lis) == type([]), "setActive is not List"
        while (len(active_lis) >= 30):
            del active_lis[0]
        self._useractive["active"] = active_lis
        self._useractive.setdefault("measure", {})
        # 计算最近多少天没来
        active_interval = 0
        for i in range(1, len(active_lis)+1):
            if active_lis[len(active_lis)-i] != 0:
                break
            active_interval = 0
        self._useractive["measure"]["lastActiveInterval"] = active_interval
        self._useractive["measure"]["last7ActiveNum"] = sum(active_lis[-7:])
        self._useractive["measure"]["last14ActiveNum"] = sum(active_lis[-14:])
        self._useractive["measure"]["last28ActiveNum"] = sum(active_lis[-28:])
        self._useractive["measure"]["last30ActiveNum"] = sum(active_lis[-30:])


    def setPartitionDate(self, partition_data):
        partition_data = partition_data.replace("-", "")
        self._useractive["partition_date"] = partition_data

    def setJhdUid(self, userkey):
        self._useractive["jh_uid"] = userkey.strip()

    def setFirstLoginTime(self, firstLoginTime):
        self._useractive.setdefault("measure", {}).setdefault("firstLoginTime", firstLoginTime)

    def setLastLoginTime(self, lastLoginTime):
        self._useractive.setdefault("measure", {}).setdefault("firstLoginTime", lastLoginTime)

    def setMeasure(self, measure):
        self._useractive["measure"] = measure

    def mergeUserActive(self, _old, _new = None):
        _new = self._user if _new is None else _new
        return dict(_old, **_new)

    def build(self):
        return self._useractive

    def builder(self):
        return self.build()

    def merge(self, _old, _new = None):
        return self.mergeUserActive(_old, _new)
