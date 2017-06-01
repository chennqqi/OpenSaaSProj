# -*- coding: utf-8 -*-
import __init__
from __init__ import configPath
from ModeWriter import ModeWriter
from pymongo.operations import *
from bson import ObjectId
from SaaSTools.tools import getDay
from SaaSTools.tools import getDayDelta
from SaaSMode.UserActiveBuilder import UserActiveBuilder
from DBClient.PyMongoClient import PyMongoClient
from collections import OrderedDict
import time


# 替换collection中的一条记录
def replace_onebyone(data, appkey, modename, client):
    conn = client.getConn()
    for key in data:
        try:
            if "_id" in data[key]:
                conn[appkey][modename].save(data[key])
            else:
                raise Exception("lack '_id' key, can not execute 'save' operate.")
        except:
            import traceback
            print(traceback.print_exc())


# 在mongodb中，创建配置文件中设置的索引
def createIndex(dbname, collection, client):
    # collection = collection.lower()
    try:
        import json
        import ConfigParser
        cf = ConfigParser.ConfigParser()
        cf.read(configPath)
        indexes = {}
        for collectionName, content in cf.items("mongodbIndex"):
            if "." in collectionName:
                if collectionName.split(".")[0] == dbname.lower() \
                        and collectionName.split(".")[1] == collection.lower():
                    index_dic = json.loads(content)
                    indexes[collectionName.split(".")[1]] = index_dic
            elif collectionName == collection.lower():
                index_dic = json.loads(content)
                indexes.setdefault(collectionName, index_dic)
        for key in indexes:
            for index in indexes[key]:
                # print(key, index, indexes[key][index])
                client.createIndex(dbname, collection, indexes[key][index])
    except:
        import traceback
        print(traceback.print_exc())
    # dropIndex(dbname, collection, client)


def dropIndex(dbname, collection, client):
    try:
        import json
        import ConfigParser
        cf = ConfigParser.ConfigParser()
        cf.read(configPath)
        indexes = {}
        for collectionName, content in cf.items("mongodbIndexDrop"):
            if "." in collectionName:
                if collectionName.split(".")[0] == dbname.lower() \
                        and collectionName.split(".")[1] == collection.lower():
                    index_dic = json.loads(content)
                    indexes[collectionName.split(".")[1]] = index_dic
            elif collectionName == collection.lower():
                index_dic = json.loads(content)
                indexes.setdefault(collectionName, index_dic)
        for key in indexes:
            for index in indexes[key]:
                client.dropIndex(dbname, collection, indexes[key][index])
    except:
        import traceback
        print(traceback.print_exc())


# mongodb 从配置文件中获取文档保留天数
def getRetention(dbname, collection):
    # collection = collection.lower()
    try:
        import json
        import ConfigParser
        cf = ConfigParser.ConfigParser()
        cf.read(configPath)
        common_retention = []
        appoint_retention = []
        for collectionName, content in cf.items("mongodbRetention"):
            if "." in collectionName:
                if collectionName.split(".")[0] == dbname.lower() and collectionName.split(".")[1] == collection.lower():
                    retention_str = cf.get("mongodbRetention", collectionName)
                    retention_data = json.loads(retention_str)
                    retention = retention_data["retention"]
                    date_field_name = retention_data["date_field_name"]
                    data_partition_format = retention_data["data_partition_format"]
                    appoint_retention = [retention, date_field_name, data_partition_format]
            elif collectionName == collection.lower():
                retention_str = cf.get("mongodbRetention", collectionName)
                retention_data = json.loads(retention_str)
                retention = retention_data["retention"]
                date_field_name = retention_data["date_field_name"]
                data_partition_format = retention_data["data_partition_format"]
                common_retention = [retention, date_field_name, data_partition_format]
        return appoint_retention if appoint_retention else common_retention
    except:
        import traceback
        print(traceback.print_exc())


# mode in ["$eq", "$lte", "$gte", "$lt", "$gt"]
def finallyMask(dbname, collection, client, mode="$eq"):
    # collection = collection.lower()
    cur_tm = time.strftime("%H%M", time.localtime(time.time()))
    conn = client.getConn()
    # 创建索引
    try:
        if cur_tm == "0000":
            createIndex(dbname, collection, client)
    except:
        import traceback
        print(traceback.print_exc())
    try:
        if cur_tm == "0000":
            dropIndex(dbname, collection, client)
    except:
        import traceback
        print(traceback.print_exc())
    # 删除数据
    try:
        if cur_tm == "0000":
            retention, date_name, date_format = getRetention(dbname, collection)
            if retention > 0:
                remove_day = time.strftime(date_format, time.localtime(time.time() - retention * 86400))
                conn[dbname][collection].remove({date_name: {mode: remove_day}})
    except:
        import traceback
        print(traceback.print_exc())


class UserIP(ModeWriter):
    '''
    {
        _id: [ip],
        inctag: [number],
        timestamp: [number]
    }
    '''

    def __init__(self, client=None):
        if client is None:
            self.client = PyMongoClient()
        else:
            self.client = client
        self.dbname = "jh"
        self.modename = "UserIP"

    def setClient(self, client):
        self.client = client

    def remove(self, *args, **kwargs):
        pass

    def write(self, data, appkey, modename, modetools, *args, **kwargs):
        modename = self.modename
        ip_set = set()
        for key in data:
            # uid = key
            for ip in data[key]["jhd_ip"]:
                ip_set.add(ip)
        op = []
        for ip in ip_set:
            query_update = {"$set": {"timestamp": time.time()}, "$inc": {"inctag": 1}, "$addToSet": {"appkey": appkey}}
            op.append(UpdateOne({"_id": ip}, query_update, upsert=True))
        try:
            if op:
                self.client.bulkWrite(self.dbname, modename, op)
        except:
            print("Warn: bulkStore 'jh.UserIP' Rise a error; Switch to Single Mode")
            for op_item in op:
                try:
                    self.client.bulkWrite(self.dbname, modename, [op_item])
                except:
                    import traceback
                    print(traceback.print_exc())


class UserCrumbsWriter(ModeWriter):

    def __init__(self, mongo_id = 1):
        self.client = PyMongoClient(mongo_id=mongo_id)
        self.conn = self.client.getConn()
        self.modename = "uvfile"
        # self.store_attachmode = UserIP()
        self.attachmode_storers = []
        try:
            # self.attachmode_storers = [UserIP(), UserProfileUpdateWriter()]
            self.attachmode_storers = [UserIP()]
        except:
            import traceback
            print(traceback.print_exc())

    def setClient(self, client):
        self.client = client
        self.conn = self.client.getConn()

    def remove(self, appkey, modename, tm):
        modename = self.modename
        tm = tm.replace("-", "")
        tm = time.strftime("%Y-%m-%d", time.localtime(time.mktime(time.strptime(tm, "%Y%m%d"))))
        self.client.remove(appkey, modename, {"tm": tm})

    def getMeasure(self, activelifeabs, fix_deltaday):
        activelifeabs = [i for i in activelifeabs if i <= fix_deltaday]
        measure = {
            "last7ActiveNum": 0,
            "last14ActiveNum": 0,
            "last28ActiveNum": 0,
            "last30ActiveNum": 0,
        }
        for activelifeabs_delta in activelifeabs:
            delta = fix_deltaday - activelifeabs_delta
            if delta <= 6:
                measure["last7ActiveNum"] += 1
            if delta <= 13:
                measure["last14ActiveNum"] += 1
            if delta <= 27:
                measure["last28ActiveNum"] += 1
            if delta <= 29:
                measure["last30ActiveNum"] += 1
        return measure

    def write(self, data, appkey, modename, modetools, *args, **kwargs):
        modename = self.modename
        # today = kwargs["today"] if "today" in kwargs else time.strftime("%Y-%m-%d", time.localtime(time.time()-86400))
        today = kwargs["today"]
        today = today.replace("-", "")
        uids = data.keys()
        yesterday = time.strftime("%Y-%m-%d", time.localtime(time.mktime(time.strptime(today, "%Y%m%d"))-86400))
        yyyy_mm_dd = time.strftime("%Y-%m-%d", time.localtime(time.mktime(time.strptime(yesterday.replace("-", ""), "%Y%m%d"))+86400))

        uvfile = self.client.find(appkey, "uvfile", OrderedDict([("tm", yyyy_mm_dd), ("jhd_userkey", {"$in": uids})]))

        user_profile = self.client.find(appkey, "UserProfile", {"_id": {"$in": uids}})

        ips = set()
        ip_loc = {}
        try:
            for uid in data:
                ips = ips.union(data[uid].get("jhd_ip"))
            ip_loc_cur = self.conn["jh"]["UserIP"].find(OrderedDict([("_id", {"$in": list(ips)}), ("province", {"$exists": True}), ("city", {"$exists": True})]), {"province": True, "city": True})
            for item in ip_loc_cur:
                ip = item["_id"]
                province = item["province"]
                city = item["city"]
                if not province:
                    continue
                if not city:
                    city = province
                # ip_loc.setdefault(ip, "_".join([province, city]))
                ip_loc.setdefault(ip, {"prov": province, "city": city})
        except:
            import traceback
            print traceback.print_exc()

        # 合并 数据
        for doc in uvfile:
            uid = doc["jhd_userkey"]
            data[uid] = modetools.mergeUserCrumbs(doc, data[uid])
            try:
                ip_lis = data[uid]["jhd_ip"]
                data[uid].setdefault("jhd_loc", [])

                for ip in ip_lis:
                    loc = ip_loc.get(ip, None)
                    if loc and loc not in data[uid]["jhd_loc"]:
                        data[uid]["jhd_loc"].append(loc)
                # tmp = []
                # for item in data[uid]["jhd_loc"]:
                #     if isinstance(item, dict):
                #         tmp.append(item)
                # data[uid]["jhd_loc"] = tmp
            except:
                import traceback
                print traceback.print_exc()

        fix_deltaday = getDayDelta(today, "20160101")

        # lastActiveInterval
        # firstLoginTime
        for doc in user_profile:
            # print("doc", doc["_id"], fix_deltaday, doc.get("activelifeabs", []))
            key = doc["_id"]
            tmp = {}
            # 获取用户首次登录时间
            tmp["firstLoginTime"] = doc.get("firstLoginTime", "unknown")
            firstloginday = tmp["firstLoginTime"][:8]
            activelifeabs = doc.get("activelifeabs", [])
            # 获取用户最近最近活跃信息
            tmp["measure"] = self.getMeasure(activelifeabs, fix_deltaday)
            tmp["measure"]["firstLoginTime"] = tmp["firstLoginTime"]
            # 更新数据
            data[key] = dict(data[key], **tmp)
        op = []
        for key in data:

            if "_id" not in data[key]:
                data[key]["_id"] = ObjectId()
            _id = data[key]["_id"]
            op.append(ReplaceOne({"_id": _id}, modetools.formatList(data[key]), True))

        try:
            if op:
                self.client.bulkWrite(appkey, modename, op)
        except:
            print("Warn: bulkStore 'uvfile' Rise a error; Switch to Single Mode")
            try:
                replace_onebyone(data, appkey, modename, self.client)
            except:
                import traceback
                print(traceback.print_exc())
        finallyMask(appkey, modename, self.client)
        try:
            kwargs["ip_loc"] = ip_loc
            self.store_attachmode(data, appkey, modename, modetools, *args, **kwargs)
        except:
            import traceback
            print traceback.print_exc()


class UserProfileUpdateWriter(ModeWriter):

    def __init__(self):
        self.client = PyMongoClient()
        self.conn = self.client.getConn()
        self.modename = "UserProfile"

    def setClient(self, client):
        self.client = client
        self.conn = self.client.getConn()

    def remove(self, appkey, modename, tm):
        pass

    def write(self, data, appkey, modename, modetools, *args, **kwargs):
        modename = self.modename
        a = time.time()

        ip_loc = kwargs["ip_loc"]
        curDay = kwargs["today"].replace("-", "")
        # fix_deltaday = getDayDelta(curDay, "20160101")
        for uid in data:
            for ip in data[uid]["jhd_ip"]:
                loc = ip_loc.get(ip, {})
                if not loc:
                    continue
                prov = loc.get("prov", "#")
                city = loc.get("city", "#")

                loc_data = {"prov": prov, "city": city}

                data[uid].setdefault("locs", []).append(loc_data)

        op = []
        for uid in data:
            locs = data[uid].get("locs", [])
            if not locs:
                continue
            for loc in locs:
                op.append(UpdateOne({"_id": uid}, {"$addToSet": {"locs": loc}}))

        try:
            if op:
                self.client.bulkWrite(appkey, modename, op)
        except:
            print("Warn: bulkStore 'UserProfile' Rise a error; Switch to Single Mode")
            try:
                replace_onebyone(data, appkey, modename, self.client)
            except:
                import traceback
                print(traceback.print_exc())
        print("UserProfileUpdateWriter cost seconds %.3f" % ((time.time() - a),))



class UserProfileWriter(ModeWriter):

    def __init__(self, mongo_id = 1):
        self.client = PyMongoClient(mongo_id=mongo_id)
        self.conn = self.client.getConn()
        self.modename = "UserProfile"

    def setClient(self, client):
        self.client = client
        self.conn = self.client.getConn()

    def remove(self, appkey, modename, tm):
        pass

    def write(self, data, appkey, modename, modetools, *args, **kwargs):
        a = time.time()
        curDay = kwargs["today"].replace("-", "")
        fix_deltaday = getDayDelta(curDay, "20160101")
        modename = self.modename
        uids = data.keys()

        docs = self.client.find(appkey, modename, {"_id": {"$in": uids}})

        oldusers = set()
        for doc in docs:
            try:
                # UserProfile _id 为 userkey
                key = doc["_id"]
                oldusers.add(key)
                # 如果新添加数据比首次访问时间要早，对历史数据进行修正处理
                if "lastLoginTime" in data[key]:
                    lastLoginTime_new = data[key]["lastLoginTime"][:8]
                if "firstLoginTime" in doc and "firstLoginTime" in data[key]:
                    activelife = doc.get("activelife", [0])
                    firstLoginTime_new = data[key]["firstLoginTime"][:8]
                    firstLoginTime_old = doc["firstLoginTime"][:8]
                    if firstLoginTime_new < firstLoginTime_old:
                        firstLoginDelta = getDayDelta(firstLoginTime_old, firstLoginTime_new)
                        doc["activelife"] = map(lambda i: i+firstLoginDelta, activelife)

                data[key] = modetools.mergeUserProfile(data[key], doc)
                # 生成用户生命周期数据
                firstLoginDay = data[key]["firstLoginTime"][:8]
                lastLoginDay = data[key]["lastLoginTime"][:8]
                dayDelta = getDayDelta(lastLoginTime_new, firstLoginDay)
                data[key].setdefault("activelife", [0]) # 兼容历史数据
                if dayDelta not in data[key]["activelife"]:
                    data[key]["activelife"].append(dayDelta)
                data[key]["activelife"].sort()
                # 用户绝对活跃数据，起始 日期为 2016-01-01
                try:
                    firstlogin_deltaday = getDayDelta(firstLoginDay, "20160101")
                    data[key]["activelifeabs"] = [firstlogin_deltaday + remain_day for remain_day in data[key]["activelife"]]
                except:
                    import traceback
                    print(traceback.print_exc())
            except:
                import traceback
                print(traceback.print_exc())
        # 设置新增版本
        # for key in set(uids)-set([item["_id"] for item in docs]):
        for key in set(uids)-oldusers:
            data[key]["comever"] = data[key]["ver"]
            # 用户绝对活跃数据，起始 日期为 2016-01-01
            try:
                firstLoginDay = data[key]["lastLoginTime"][:8]
                firstlogin_deltaday = getDayDelta(firstLoginDay, "20160101")
                data[key]["activelifeabs"] = [firstlogin_deltaday + remain_day for remain_day in
                                              data[key]["activelife"]]
            except:
                import traceback
                print(traceback.print_exc())

        op = []
        for key in data:
            op.append(ReplaceOne({"_id": key}, data[key], True))
        try:
            if op:
                self.client.bulkWrite(appkey, modename, op)
        except:
            print("Warn: bulkStore 'UserProfile' Rise a error; Switch to Single Mode")
            try:
                replace_onebyone(data, appkey, modename, self.client)
            except:
                import traceback
                print(traceback.print_exc())
        finallyMask(appkey, modename, self.client)
        print("UserProfileWriter cost seconds %.3f" % ((time.time() - a),))


class UserEventWriter(ModeWriter):

    def __init__(self):
        self.client = PyMongoClient()
        self.groupwriter = UserEventGroupWriter()
        self.modename = "UserEvent"

    def setClient(self, client):
        self.client = client

    def remove(self, appkey, modename, tm):
        modename = self.modename
        tm = tm.replace("-", "")
        # 格式化
        tm = time.strftime("%Y%m%d", time.localtime(time.mktime(time.strptime(tm, "%Y%m%d"))))
        self.client.remove(appkey, modename, {"partition_date": tm})
        self.groupwriter.remove(appkey, modename, tm)

    def write(self, data, appkey, modename, modetools, *args, **kwargs):
        a = time.time()
        modename = self.modename
        op = []
        for doc in data:
            try:
                # doc["_id"] = ObjectId()
                doc["partition_date"] = kwargs["today"].replace("-", "") \
                    if "today" in kwargs else time.strftime("%Y%m%d", time.localtime(time.time()-86400))
                jhd_userkey = doc["jhd_userkey"]
                jhd_ts = doc["jhd_ts"]
                jhd_eventId = doc["jhd_eventId"]
                _id = "_".join(map(str, [jhd_userkey, jhd_ts, jhd_eventId[:10]]))
                doc["_id"] = _id
                # op.append(InsertOne(doc))
                op.append(ReplaceOne({"_id": _id}, doc, True))
            except:
                import traceback
                print(traceback.print_exc(), doc)
        try:
            if op:
                op = self.client.bulkWrite(appkey, modename, op)
        except:
            print("Warn: bulkStore 'UserProfile' Rise a error; Switch to Single Mode")
            for op_item in op:
                try:
                    self.client.bulkWrite(appkey, modename, [op_item])
                except:
                    import traceback
                    print(traceback.print_exc())
        finallyMask(appkey, modename, self.client)
        self.groupwriter.write(data, appkey, modename, modetools, *args, **kwargs)
        print("UserEventWriter cost seconds %.3f" % ((time.time() - a),))


class UserEventGroupWriter(ModeWriter):

    def __init__(self):
        self.client = PyMongoClient()
        self.modename = "UserEventGroup"

    def setClient(self, client):
        self.client = client

    def remove(self, appkey, modename, tm):
        modename = self.modename
        tm = tm.replace("-", "")
        # 格式化
        tm = time.strftime("%Y%m%d", time.localtime(time.mktime(time.strptime(tm, "%Y%m%d"))))
        self.client.remove(appkey, modename, {"partition_date": tm})

    def write(self, data, appkey, modename, modetools, *args, **kwargs):
        a = time.time()
        modename = self.modename
        op = []
        for doc in data:
            try:
                doc_new = {}
                # doc["_id"] = ObjectId()
                doc["partition_date"] = kwargs["today"].replace("-", "") \
                    if "today" in kwargs else time.strftime("%Y%m%d", time.localtime(time.time() - 86400))
                jhd_userkey = doc["jhd_userkey"]
                jhd_eventId = doc["jhd_eventId"]
                jhd_opType = doc["jhd_opType"]
                jhd_vr = doc["jhd_vr"]
                jhd_pb = doc["jhd_pb"]
                if jhd_opType not in ["action", "page"]:
                    continue
                _id = "_".join(map(str, [jhd_userkey, jhd_eventId[:10], doc["partition_date"]]))
                doc_new["_id"] = _id
                doc_new["jhd_userkey"] = jhd_userkey
                doc_new["jhd_eventId"] = jhd_eventId
                doc_new["jhd_opType"] = jhd_opType
                doc_new["jhd_vr"] = jhd_vr
                doc_new["jhd_pb"] = jhd_pb
                doc_new["partition_date"] = doc["partition_date"]
                # op.append(InsertOne(doc))
                op.append(ReplaceOne({"_id": _id}, doc_new, True))
            except:
                import traceback
                print(traceback.print_exc(), doc)
        try:
            if op:
                op = self.client.bulkWrite(appkey, modename, op)
        except:
            print("Warn: bulkStore 'UserProfile' Rise a error; Switch to Single Mode")
            for op_item in op:
                try:
                    self.client.bulkWrite(appkey, modename, [op_item])
                except:
                    import traceback
                    print(traceback.print_exc())
        finallyMask(appkey, modename, self.client)
        print("UserEventGroupWriter cost seconds %.10f" % ((time.time() - a),))

# 没有实时计算（按分钟）
class UserActiveWriter(ModeWriter):

    def __init__(self):
        self.client = PyMongoClient()
        self.modename = "UserActive"

    def setClient(self, client):
        self.client = client

    def remove(self, appkey, modename, tm):
        modename = self.modename
        tm = tm.replace("-", "")
        # 格式化
        tm = time.strftime("%Y%m%d", time.localtime(time.mktime(time.strptime(tm, "%Y%m%d"))))
        self.client.remove(appkey, modename, {"partition_date": tm})


    # def write(self, appkey, modename, curDay=time.strftime("%Y-%m-%d", time.localtime(time.time()-86400)), *args, **kwargs):
    def write(self, data, appkey, modename, modetools, *args, **kwargs):
        a = time.time()
        modename = self.modename
        curDay = kwargs["today"].replace("-", "") if "today" in kwargs else time.strftime("%Y%m%d", time.localtime(time.time()-86400))
        conn = self.client.getConn()
        userActiveCollection = conn[appkey][modename]
        docs = self.client.find(appkey, "UserProfile", {})
        yesterday = getDay(curDay, "%Y%m%d", -1)
        op = []
        a = time.time()
        for doc in docs:
            try:
                key = doc["_id"]
                activelife = doc.get("activelife", [0])
                firstLoginTime = doc["firstLoginTime"][:8]
                login = getDayDelta(curDay, firstLoginTime) in activelife
                # 查找前一天的用户活跃记录,需要配合索引提升速度db.UserActive.ensureIndex({partition_date: -1, jh_uid: 1})
                userActive = userActiveCollection.find_one({"jh_uid": key, "partition_date": yesterday})
                # 构造今天的用户记录
                newUserActive = UserActiveBuilder()
                newUserActive.setJhdUid(key)
                newUserActive.setPartitionDate(curDay)
                if userActive is None:
                    newUserActive.setActive([1] if login else [0])
                else:
                    userActive["active"].append(1 if login else 0)
                    newUserActive.setActive(userActive["active"])
                # 计算衡量指标
                newUserActive.setFirstLoginTime(doc["firstLoginTime"])
                newUserActive.setLastLoginTime(doc["lastLoginTime"])
                op.append(ReplaceOne({"jh_uid": key, "partition_date": curDay}, newUserActive.builder(), upsert=True))
            except:
                import traceback
                print(traceback.print_exc(), doc)
        print("find cost time: %d" % int(time.time()-a))
        # print("len(op): ", len(op), "yesterday: ", yesterday)
        try:
            if op:
                userActiveCollection.bulk_write(op)
        except:
            import traceback
            print(traceback.print_exc())
            print("Warn: bulkStore 'UserActive' Rise a error; Switch to Single Mode")
            for op_item in op:
                try:
                    userActiveCollection.bulk_write([op_item])
                except:
                    import traceback
                    print(traceback.print_exc())
        finallyMask(appkey, modename, self.client)
        print("UserActiveWriter cost seconds %.10f" % ((time.time() - a),))


# 没有实时计算（按分钟）
class UserActiveUpdateWriter(ModeWriter):

    def __init__(self):
        self.client = PyMongoClient()
        self.modename = "UserActiveUpdate"

    def setClient(self, client):
        self.client = client

    def remove(self, appkey, modename, tm):
        pass

    def write(self, data, appkey, modename, modetools, *args, **kwargs):
        a = time.time()
        modename = self.modename
        curDay = kwargs["today"].replace("-", "")

        # self.client.getConn()[appkey][modename].remove({})
        # data_count = self.client.getConn()[appkey][modename].find({}).count()
        # if data_count == 0:
        #     data_cur = self.client.getConn()[appkey]["UserActive"].find({"partition_date": "20161114"}, {"jh_uid": 1})
        #     for item in data_cur:
        #         self.client.getConn()[appkey][modename].insert({"_id": item["jh_uid"]})

        activelifeabs = getDayDelta(curDay, "20160101")
        update_query = {
            "$addToSet": {"activelifeabs": activelifeabs}
        }
        op = []
        for key in data:
            # key is userkey
            try:
                uid = key
                op.append(UpdateOne({"_id": uid}, update_query, True))
            except:
                import traceback
                print(traceback.print_exc(), key, data[data])
        try:
            if op:
                op = self.client.bulkWrite(appkey, modename, op)
        except:
            import traceback
            print(traceback.print_exc())
            print("Warn: bulkStore 'UserActive' Rise a error; Switch to Single Mode")
            for op_item in op:
                try:
                    self.client.bulkWrite(appkey, modename, [op_item])
                except:
                    import traceback
                    print(traceback.print_exc())
        finallyMask(appkey, modename, self.client)
        print("UserActiveUpdateWriter cost seconds %.10f" % ((time.time() - a),))


class UserMapMetaWriter(ModeWriter):

    def __init__(self, mongo_id=1):
        self.client = PyMongoClient(mongo_id=mongo_id)
        self.modename = "UserMapMeta"

    def setClient(self, client):
        self.client = client

    def remove(self, appkey, modename, tm):
        modename = "UserMapMeta"
        pass

    def write(self, data, appkey, modename, modetools, *args, **kwargs):
        a = time.time()
        # self.client.getConn()[appkey][modename].remove({})
        modename = self.modename
        op = []
        for _data in data:
            try:
                if not _data:
                    continue
                update_query = {
                    "$addToSet": {"fields": {"$each": _data.pop("fields")}}
                }
                _id = _data.pop("_id")
                op.append(UpdateOne({"_id": _id}, update_query, True))
                for key in _data["field_elems"].keys():
                    update_query = {
                        "$addToSet": {"field_elems.%s" % key: {"$each": [item for item in _data["field_elems"].pop(key)]}}
                    }
                    # import json
                    # print "-"*100, json.dumps(update_query)
                    # 元素个数 >100 的不更新
                    op.append(UpdateOne({"_id": _id, "field_elems.%s.100" % (key,): {"$exists": False}}, update_query, True))
            except:
                import traceback
                print(traceback.print_exc(), op)
        try:
            if op:
                op = self.client.bulkWrite(appkey, modename, op)
        except:
            print("Warn: bulkStore 'UserProfile' Rise a error; Switch to Single Mode")
            for op_item in op:
                try:
                    self.client.bulkWrite(appkey, modename, [op_item])
                except:
                    import traceback
                    print(traceback.print_exc())
        # self.client.getConn()[appkey][modename].remove({"field_elems": {"$exists": True}})
        finallyMask(appkey, modename, self.client)
        print("UserMapMetaWriter cost seconds %.10f" % ((time.time() - a),))


if __name__ == "__main__":
    client = PyMongoClient()
    uewriter = dropIndex("caiyu_ios_free", "UserEvent", client)
    # uewriter.remove("feeling", )