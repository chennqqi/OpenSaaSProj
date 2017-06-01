# -*- coding: utf-8 -*-
from datetime import date
from pony.orm import *


def define_plat_event(db):

    class Plat_event(db.Entity):
        id = PrimaryKey(int, sql_type='int(20)', auto=True)
        tm = Required(date)
        event = Required(str)
        ver = Required(str)
        pub = Required(str)
        uv = Optional(int)
        pv = Optional(int)

    return Plat_event


if __name__ == "__main__":
    a = Database()
    define_plat_event(a)
    a.bind("mysql", host="outjhkj01.mysql.rds.aliyuncs.com", port=3306, user="jhkj", passwd="jhkj_jhkj", db="saas_meta")
    a.generate_mapping(create_tables=True)

    b = Database()
    define_plat_event(b)
    b.bind("mysql", host="outjhkj01.mysql.rds.aliyuncs.com", port=3306, user="jhkj", passwd="jhkj_jhkj", db="guaengdemo")

    a.disconnect()
    b.disconnect()

    b.generate_mapping(create_tables=True)
    # db.drop_table("plat_event")
    # tester = Plat_event()
    # b = Database()
    # setDB(b)
    # db.bind("mysql", host="outjhkj01.mysql.rds.aliyuncs.com", port=3306, user="jhkj", passwd="jhkj_jhkj", db="guaengdemo")
    # db.generate_mapping(create_tables=True)

    # tester = Plat_event()