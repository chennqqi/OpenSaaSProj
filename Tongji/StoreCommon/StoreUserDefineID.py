# -*- coding: utf-8 -*-
import __init__
from Tongji.AnalysisCommon.UserDefineID import UserDefineID
# from datetime import datetime
import datetime
from SaaSCommon.JHDecorator import fn_timer
from Tongji.Mode import PlatUserDefineID
from Tongji.Mode.bindTable import bindTable
from pony.orm import *
from StoreBasic import StoreBasic


class StoreUserDefineID(StoreBasic):

    def __init__(self):
        pass

    @fn_timer
    def store(self, num, dbname, datatype, plat, mainname=["eventid_udf", "pageid_udf"]):
        # datatype = datatype.lower()
        tm = datetime.datetime.now()
        db_eventid = Database()
        db_pageid = Database()
        plat_eventid_table = PlatUserDefineID.define_plat_event_id(db_eventid)
        plat_pageid_table = PlatUserDefineID.define_plat_page_id(db_pageid)
        tablename_eventid = "%(datatype)s_%(plat)s_%(mainname)s" % \
                    {"datatype": datatype.lower(), "plat": plat.lower(), "mainname": mainname[0]}
        tablename_pageid = "%(datatype)s_%(plat)s_%(mainname)s" % \
                            {"datatype": datatype.lower(), "plat": plat, "mainname": mainname[1]}
        db_eventid = bindTable(db_eventid, plat_eventid_table, dbname, tablename_eventid)
        db_pageid = bindTable(db_pageid, plat_pageid_table, dbname, tablename_pageid)
        tongji = UserDefineID()
        result = tongji.getData(datatype, num)
        with db_session:
            for optype in result:
                if optype == "action":
                    for eventid in result[optype]:
                        if not plat_eventid_table.exists(id=eventid):
                            plat_eventid_table(inserttm=tm, id=eventid, name=eventid, isdel=0)
                elif optype == "page":
                    for pageid in result[optype]:
                        if not plat_pageid_table.exists(id=pageid):
                            plat_pageid_table(inserttm=tm, id=pageid, name=pageid, isdel=0)
        db_eventid.disconnect()
        db_pageid.disconnect()


if __name__ == "__main__":
    tester = StoreUserDefineID()




