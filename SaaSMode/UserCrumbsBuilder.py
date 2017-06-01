# -*- coding: utf-8 -*-
import itertools
from BasicMode import BasicMode


class UserCrumbsBuilder(BasicMode):

    def __init__(self):
        self._user = {}
        self._user.setdefault("item_count", {})
        self._user.setdefault("item_add", {})
        # self.curct = time.strftime("%Y%m%d%H%M%S", time.localtime(time.time()))

    def setDatatype(self, datatype):
        if not (datatype and datatype != "null"):
            return
        self._user.setdefault("jhd_datatype", datatype)

    def setUserkey(self, userkey):
        if not (userkey and userkey != "null"):
            return
        self._user.setdefault("jhd_userkey", userkey.strip())

    def setPushId(self, pushid):
        if not (pushid and pushid != "null"):
            return
        self._user.setdefault("jhd_pushid", pushid)

    def setTM(self, tm):
        if not (tm and tm != "null"):
            return
        self._user.setdefault("tm", tm)

    # 集合类型，需要转为列表
    def setOS(self, os):
        if not (os and os != "null"):
            return
        # if os not in self._user.setdefault("jhd_os", []):
        self._user.setdefault("jhd_os", set()).add(os)

    def mergeOS(self, _old, _new):
        return set(_new.get("jhd_os", set())) | set(_old.get("jhd_os", set()))

    # 集合类型，需要转为列表
    def setPub(self, pub):
        if not (pub and pub != "null"):
            return
        self._user.setdefault("jhd_pb", set()).add(pub)

    def mergePub(self, _old, _new):
        _old["jhd_pb"] = set(_old.get("jhd_pb", set())) | set(_new.get("jhd_pb", set()))
        return _old

    # 集合类型，需要转为列表
    def setNet(self, net):
        if not (net and net != "null"):
            return
        self._user.setdefault("jhd_netType", set()).add(net)

    def mergeNet(self, _old, _new):
        _old["jhd_netType"] = set(_old.get("jhd_netType", set())) | set(_new.get("jhd_netType", set()))
        return _old

    # 集合类型，需要转为列表
    def setVr(self, vr):
        if not (vr and vr != "null"):
            return
        self._user.setdefault("jhd_vr", set()).add(vr)

    def mergeVr(self, _old, _new):
        _old["jhd_vr"] = set(_old.get("jhd_vr", set())) | set(_new.get("jhd_vr", set()))
        return _old

    # 集合类型，需要转为列表
    def setIP(self, ip):
        if not (ip and ip != "null"):
            return
        self._user.setdefault("jhd_ip", set()).add(ip)

    def mergeIP(self, _old, _new):
        _old["jhd_ip"] = set(_old.get("jhd_ip", set())) | set(_new.get("jhd_ip", set()))
        return _old

    # 集合类型，需要转为列表
    def setUA(self, ua):
        if not (ua and ua != "null"):
            return
        self._user.setdefault("jhd_ua", set()).add(ua)

    def mergeUA(self, _old, _new):
        _old["jhd_ua"] = set(_old.get("jhd_ua", set())) | set(_new.get("jhd_ua", set()))
        return _old

    # 集合类型，需要转为列表
    def setItemCount(self, _id, _type, ct, usermap, interval=0, isadd=True):
        if (not _id) or _id == "null":
            return
        __id = _id.replace(".", "_").replace(" ", "+").strip("$")
        if ct not in self._user.setdefault("item_count", {}).get(__id, {}).get("opatm", []):
            self._user.setdefault("item_count", {}).setdefault(__id, {}).setdefault("eventtype", _type)
            self._user.setdefault("item_count", {}).setdefault(__id, {}).setdefault("opatm", []).append(ct)
            self._user.setdefault("item_count", {}).setdefault(__id, {}).setdefault("maps", []).append(usermap)
            self._user.setdefault("item_count", {}).setdefault(__id, {})["num"] = self._user.setdefault("item_count", {}).get(__id, {}).get("num", 0) + 1

            if ct not in self._user.setdefault("item_count", {}).setdefault(_type, {}).setdefault("opatm", []):
                self._user.setdefault("item_count", {}).setdefault(_type, {}).setdefault("eventtype", _type)
                self._user.setdefault("item_count", {}).setdefault(_type, {}).setdefault("opatm", []).append(ct)
                self._user.setdefault("item_count", {}).setdefault(_type, {})["num"] = \
                    self._user.setdefault("item_count", {}).get(_type, {}).get("num", 0) + 1
            if isadd and interval is not None:
                self.setItemAdd(__id, interval)


    def mergeItemCount(self, _old, _new):
        _old_keys = _old.get("item_count", {}).keys()
        opatm_usermap = {}
        for key in _new.get("item_count", {}).keys():
            if key not in _old_keys:
                continue
            _key = key.replace(".", "_").replace(" ", "+") # 排除mongodb的非法字符
            if _key != key:
                _old["item_count"][_key] = _old["item_count"][key]
                _new["item_count"][_key] = _new["item_count"][key]
                del _old["item_count"][key]
                del _new["item_count"][key]
            ### 添加map信息： 建立map数据，opatm_usermap ###
            for opatm, umap in zip(_old["item_count"][_key]["opatm"],
                                   _old["item_count"][_key]["maps"]
                                   if "maps" in _old["item_count"][_key]
                                   else _old["item_count"][_key].get("maps", [-1])*len(
                                       _old["item_count"][_key]["opatm"])):
                opatm_usermap[opatm] = umap
            for opatm, umap in zip(_new["item_count"][_key]["opatm"],
                                   _new["item_count"][_key]["maps"]
                                       if "maps" in _new["item_count"][_key]
                                       else _new["item_count"][_key].get("maps", [-1]) * len(
                                           _new["item_count"][_key]["opatm"])):
                opatm_usermap[opatm] = umap
            isadd = set()
            opatms = []
            usersmaps = []
            ### 添加map信息： 生成maps字段，追加新map，出现过的map仅设置首次出现的下标位置 ###
            for opatm, umap in zip(itertools.chain(_old["item_count"][_key]["opatm"], _new["item_count"][_key]["opatm"]),
                                   itertools.chain(
                                    _old["item_count"][_key]["maps"]
                                   if "maps" in _old["item_count"][_key]
                                   else _old["item_count"][_key].get("maps", [-1])*len(_old["item_count"][_key]["opatm"]),
                                   _new["item_count"][_key]["maps"]
                                    if "maps" in _new["item_count"][_key]
                                    else _new["item_count"][_key].get("maps", [-1]) * len(_new["item_count"][_key]["opatm"]))
                                   ):
                if opatm in isadd:
                    continue
                opatms.append(opatm)
                umap = opatm_usermap[opatm]
                if isinstance(umap, dict):
                    try:
                        index = usersmaps.index(umap)
                        usersmaps.append(index)
                    except ValueError:
                        usersmaps.append(umap)
                else:
                    usersmaps.append(umap)
                isadd.add(opatm)
            _old["item_count"][_key]["opatm"] = opatms[-2000:]
            _old["item_count"][_key]["maps"] = usersmaps[-2000:]
            # _old["item_count"][_key]["opatm"].sort()
            _old["item_count"][_key]["num"] = len(_old["item_count"][_key]["opatm"])
            del _new["item_count"][_key]
        _old["item_count"] = dict(_old.get("item_count", {}), **_new["item_count"])
        return _old

    # 集合类型，需要转为列表
    def setItemAdd(self, _id, interval):
        self._user.setdefault("item_add", {})[_id] = self._user.get("item_add", {}).get(_id, 0) + interval

    def mergeItemAdd(self, _old, _new):
        for key in _new.get("item_add", {}).keys():
            if key not in _old.get("item_add", {}):
                continue
            _old["item_add"][key] += _new["item_add"][key]
            del _new["item_add"][key]
        _old["item_add"] = dict(_old.get("item_add", {}), **_new["item_add"])
        return _old

    def setRandom(self, toplimit=1000, fresh=False):
        if "random" in self._user and not fresh:
            return
        import random
        self._user.setdefault("random", random.randint(1, toplimit))

    def mergeUserCrumbs(self, _old, _new=None):
        if _new is None:
            _new = self.builder()
        self.mergeIP(_old, _new)
        self.mergeNet(_old, _new)
        self.mergeOS(_old, _new)
        self.mergePub(_old, _new)
        self.mergeUA(_old, _new)
        self.mergeItemCount(_old, _new)
        self.mergeItemAdd(_old, _new)
        self.mergeLastOpaTime(_old, _new)
        return _old

    def setLastOpaTime(self, ct):
        # if ct > self.curct:
        #     return
        self._user["lastOpaTime"] = max([ct.replace(":", "").replace("-", "").replace("+", ""),
                                         self._user.get('lastOpaTime', '00000000000000')])

    def mergeLastOpaTime(self, _old, _new):
        lastOpaTime = max([_old.get("lastOpaTime", "00000000000000"),
                                         _new.get("lastOpaTime", "00000000000000")])
        _old["lastOpaTime"] = lastOpaTime
        return _old

    def builder(self):
        self.setRandom()
        return self._user

    def merge(self, _old, _new = None):
        return self.mergeUserCrumbs(_old, _new)

    def formatList(self, data=None, keys = ["jhd_os", "jhd_pb", "jhd_ua", "jhd_netType", "jhd_vr", "jhd_ip"]):
        if data is None:
            data = self._user
        for key in keys:
            if key in data:
                data[key] = list(data[key])


if __name__ == "__main__":
    tester = UserCrumbsBuilder()
    a = {"jhd_pb": set(["AppStore"]), "jhd_datatype": "feeling", "jhd_netType": set(["4g", "wifi"]), "jhd_vr": ["2.0.2"], "jhd_userkey": "2b26ae70-1a93-4c5f-83ce-44e3fe7a7826", "random": 287, "jhd_os": ["iphone_9.3.4"], "item_count": {"ac56": {"eventtype": "page", "opatm": ["00:01:36", "00:07:43", "00:07:49", "00:07:50", "00:07:53", "00:08:12", "00:08:35", "00:12:19", "00:12:24", "00:13:18", "00:13:36", "00:14:32", "00:15:11", "00:18:10", "00:18:14", "00:18:19", "00:18:22", "00:18:38", "00:19:58", "00:21:51", "00:22:11", "00:22:59", "00:23:28", "00:23:31", "00:23:52", "00:24:08", "00:24:10", "00:24:29", "00:24:33", "00:25:39", "00:25:43", "00:25:47", "00:25:52", "00:37:21", "00:38:29", "00:41:39", "00:41:51", "00:46:15", "00:46:19", "00:49:24", "00:49:30", "00:54:28", "00:55:23", "12:05:37", "12:22:13", "12:22:20", "12:22:31", "23:59:46"], "num": 48}, "ac44": {"eventtype": "action", "opatm": ["00:43:48", "00:45:57", "00:50:21", "00:50:25", "00:54:36", "01:02:04", "01:08:36", "12:05:36", "23:59:46"], "num": 9}, "n": {"eventtype": "page", "opatm": ["00:01:37", "00:02:20", "00:06:30", "00:07:50", "00:07:51", "00:12:21", "00:13:19", "00:13:35", "00:14:40", "00:15:03", "00:15:09", "00:18:21", "00:18:23", "00:18:36", "00:23:29", "00:24:30", "00:25:40", "00:25:45", "00:38:30", "00:38:32", "00:41:38", "00:41:42", "00:43:47", "00:45:54", "00:46:14", "00:48:34", "00:49:20", "00:50:18", "00:50:33", "00:50:49", "00:50:56", "00:51:00", "00:51:03", "00:51:07", "00:51:10", "00:51:18", "00:51:22", "00:54:32", "00:54:46", "00:54:59", "00:55:22", "00:55:26", "00:55:59", "00:58:14", "00:58:44", "00:58:50", "00:59:00", "01:08:33", "01:29:09", "12:05:34", "12:05:39", "12:05:59", "12:22:32", "23:59:43"], "num": 54}, "ms_571c15195bbb500064e37274": {"eventtype": "page", "opatm": ["00:00:21", "00:24:15", "12:22:16", "12:22:23", "23:59:49"], "num": 5}, "in": {"eventtype": "in", "opatm": ["00:02:59", "00:03:09", "00:06:11", "00:07:41", "00:11:05", "00:12:38", "00:15:34", "00:43:38", "00:43:46", "00:58:03", "01:02:00", "01:06:07", "01:06:50", "01:13:57", "01:18:04", "01:28:44", "12:05:34", "12:22:12", "23:59:43"], "num": 19}}, "jhd_pushid": "57aeb2f8c4c9710054731ac2", "jhd_ua": set(["iPhone7_1", "iphone7_1"]), "tm": "2016-08-22", "measure": {"last14ActiveNum": 7, "lastActiveInterval": 1, "last7ActiveNum": 5, "firstLoginTime": "20160813", "lastLoginTime": "20160820200329", "last28ActiveNum": 7, "last30ActiveNum": 7}, "item_add": {"in": 2938.0185730000003}, "jhd_ip": set(["183.39.154.243", "183.43.34.139"]), "lastOpaTime": "20160821235949"}
    b = {"jhd_pb": set(["AppStore"]), "jhd_netType": set(["wifi"]), "jhd_vr": set(["2.0.2"]), "jhd_ip": set(["183.39.154.243"]), "jhd_userkey": "2b26ae70-1a93-4c5f-83ce-44e3fe7a7826", "random": 827, "jhd_os": set(["iphone_9.3.4"]), "item_count": {"ac56": {"eventtype": "action", "opatm": ["00:36:12"], "num": 1}}, "jhd_pushid": "57aeb2f8c4c9710054731ac2", "jhd_datatype": "feeling", "tm": "2016-08-15", "item_add": {}, "jhd_ua": set(["iPhone7_1"]), "lastOpaTime": "20160822003612"}
    print("ssssssss", tester.mergeItemCount(a, b))
    c = a["item_count"]
    d = b["item_count"]
    c_keys = set(c.keys())
    print(c)
    e = dict(a["item_count"], **b["item_count"])
    print(e)
    e_keys = set(e.keys())
    print(c_keys - e_keys)
    print(e_keys - c_keys)
    print(a["item_count"])
