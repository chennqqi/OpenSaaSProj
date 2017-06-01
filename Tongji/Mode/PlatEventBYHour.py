# -*- coding: utf-8 -*-
from datetime import datetime
from pony.orm import *


def define_event_rt_byhour(db):

    class Plat_event_rt_byhour(db.Entity):
        id = PrimaryKey(int, sql_type='int(20)', auto=True)
        tm = Required(datetime)
        plat = Optional(str)
        ver = Required(str)
        pub = Required(str)
        eventid = Required(str)
        uv = Optional(int)
        pv = Optional(int)
        newcomer = Optional(int)
        addnewcomer = Optional(int)
        active = Optional(int)
        addactive = Optional(int)
    return Plat_event_rt_byhour

if __name__ == "__main__":
    a = Database()
    define_event_rt_byhour(a)
    a.bind("mysql", host="outjhkj01.mysql.rds.aliyuncs.com", port=3306, user="jhkj", passwd="jhkj_jhkj", db="guaengdemo")
    a.generate_mapping(create_tables=True)

    # b = Database()
    # define_plat_event(b)
    # b.bind("mysql", host="outjhkj01.mysql.rds.aliyuncs.com", port=3306, user="jhkj", passwd="jhkj_jhkj", db="guaengdemo")

    a.disconnect()
    # b.disconnect()
    #
    # b.generate_mapping(create_tables=True)