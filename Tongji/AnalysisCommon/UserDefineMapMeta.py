# -*- coding: utf-8 -*-
from DBClient.PyMongoClient import PyMongoClient
from DBClient.MysqlClient import MysqlClient


class UserDefineMapMeta(object):

    def __init__(self):
        pass

    def get_mongoid(self, appkey):
        m_client = MysqlClient()
        self.mongo_id = m_client.get_mongoid(appkey)[0]
        m_client.closeMysql()

    def getData(self, appkey, *args, **kwargs):
        modename = "UserMapMeta"
        self.get_mongoid(appkey)
        self.client = PyMongoClient(self.mongo_id)
        cur = self.client.getConn()[appkey][modename].find({})
        result = {}
        # format: { "_id" : "ac7", "fields" : [ { "type" : "string", "name" : "type" } ], "field_elems" : { "type" : [ "分类视图" ] } }
        for item in cur:
            eventid = item["_id"]
            fields = item["fields"]
            field_elems = item.get("field_elems", {})
            # 保存 type/elems
            for field in fields:
                try:
                    mapkey = field["name"]
                    mapkey_type = field["type"]
                    elems = field_elems.get(mapkey, [])
                    elems_tmp = []
                    for elem in elems:
                        if isinstance(elem, str) or isinstance(elem, unicode):
                            # 排除URL类型属性
                            if "http" in elem:
                                continue
                            if len(elem) >= 60:
                                continue
                        elems_tmp.append(elem)
                        if len(elems_tmp) >= 100:
                            break
                    result.setdefault(eventid, {}).setdefault(mapkey,
                                                              {"type": mapkey_type,
                                                               "elems": elems_tmp,
                                                               }
                                                              )
                except:
                    continue
            # 不包含map的情况
            if bool(fields) == False:
                result.setdefault(eventid, {})
        return result

if __name__ == "__main__":
    tester = UserDefineMapMeta()
    print tester.getData("biqu")