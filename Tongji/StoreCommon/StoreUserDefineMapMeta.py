# -*- coding: utf-8 -*-
import __init__
from Tongji.AnalysisCommon.UserDefineMapMeta import UserDefineMapMeta
import datetime
import urllib
import json
from SaaSCommon.JHDecorator import fn_timer
from Tongji.Mode import PlatUserDefineMapMeta
from Tongji.Mode.bindTable import bindTable
from pony.orm import *
from StoreBasic import StoreBasic


class StoreUserDefineMapMeta(StoreBasic):

    def __init__(self):
        pass

    @fn_timer
    def store(self, num, dbname, datatype, plat, mainname="mapmeta_udf"):
        tm = datetime.datetime.now()
        db = Database()
        table = PlatUserDefineMapMeta.define_plat_mapmeta(db)
        tablename = "%(datatype)s_%(plat)s_%(mainname)s" % \
                    {"datatype": datatype.lower(), "plat": plat.lower(), "mainname": mainname}
        db = bindTable(db, table, dbname, tablename)
        tongji = UserDefineMapMeta()
        result = tongji.getData(datatype)
        with db_session:
            for key in result:
                eventid = key
                for mapkey in result[eventid]:
                    mapkey_type = result[eventid][mapkey]["type"]
                    mapkey_elems = result[eventid][mapkey]["elems"]
                    try:
                        options = json.dumps([{"key": elem, "value": elem} for elem in mapkey_elems], separators=(',', ':'), ensure_ascii=False)
                    except:
                        print json.dumps([{"key": elem, "value": elem} for elem in mapkey_elems], separators=(',', ':'), ensure_ascii=False)
                        import traceback
                        print traceback.print_exc()
                        options = "[]"
                    # 新增事件，则插入
                    if not table.exists(id=eventid, mapkey=mapkey):
                        table(inserttm=tm, id=eventid, mapkey=mapkey, keyname=mapkey, valuetype=mapkey_type, relation=mapkey_type, options=options)
                    # 已有事件更新
                    else:
                        table.get(id=eventid, mapkey=mapkey).valuetype = mapkey_type
                        table.get(id=eventid, mapkey=mapkey).relation = mapkey_type
                        table.get(id=eventid, mapkey=mapkey).options = options
                if bool(result[eventid]) == False:
                    try:
                        # 新增事件，则插入
                        if not table.exists(id=eventid, mapkey=""):
                            table(inserttm=tm, id=eventid, mapkey="", keyname="", valuetype="", relation="", options="[]")
                    except:
                        import traceback
                        print traceback.print_exc()
            db.execute("update %(db_name)s.%(table_name)s set options = NULL where options = '[]'" % {
                "db_name": dbname,
                "table_name": tablename,
            })
        db.disconnect()


if __name__ == "__main__":
    tester = StoreUserDefineMapMeta()
    tester.store(1, "jh_10a0e81221095bdba91f7688941948a6", "BIQU_ANDROID", "android")
