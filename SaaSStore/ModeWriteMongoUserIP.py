# -*- coding: utf-8 -*-
import time
from collections import OrderedDict
from pymongo.operations import *
from ModeWriterMongo import ModeWriter
from DBClient.PyMongoClient import PyMongoClient
# from SaaSCommon.JHDecorator import fn_timer


class ModeWriteMongoUserIP(ModeWriter):

    def __init__(self, mongo_id=1):
        self.client = PyMongoClient(mongo_id=mongo_id)
        self.conn = self.client.getConn()
        self.dbname = "jh"
        self.modename = "UserIP"

    def remove(self, *args, **kwargs):
        pass

    def write(self, *args, **kwargs):
        '''
        :param args: 保留参数
        :param kwargs: today = 当天日期（yyyy-mm-dd）
        :return:
        '''
        today = kwargs["today"]
        cur = self.conn[self.dbname][self.modename].find({"timestamp": {"$gte": time.time()-100}, "province": {"$exists": True}, "city": {"$exists": True}, "appkey": {"$exists": True}}, {"province": True, "city": True, "appkey": True})
        update_appkey = {}
        for item in cur:
            ip = item["_id"]
            province = item["province"]
            city = item["city"]
            if not province:
                continue
            if not city:
                city = province
            appkey = item["appkey"]
            for a_appkey in appkey:
                update_appkey.setdefault(a_appkey, []).append(
                    UpdateOne(
                        OrderedDict([("tm", today), ("jhd_loc", {"$exista": False}), ("jhd_ip", ip)]), # 需要配合索引使用提高更新速度
                        {"$addToSet": {"jhd_loc": {"prov": province, "city": city}}}
                    )
                )
        for a_appkey in update_appkey:
            a = time.time()
            self.client.bulkWrite(a_appkey, "uvfile", update_appkey[a_appkey])
            print(__name__, time.time()-a, a_appkey, len(update_appkey[a_appkey]))

if __name__ == "__main__":
    tester = ModeWriteMongoUserIP()
    tester.write(today="2016-12-27")



