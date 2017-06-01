# -*- coding: utf-8 -*-
from datetime import date
from pony.orm import *
from Tongji.CustomizeCaiyu.Mode.PlatPush import define_plat_push


def define_plat_push_week(db):

    class Caiyu_plat_push_week(db.Entity):
        tm = Optional(date)
        title = Optional(str, 2048)
        openpush = Optional(int, sql_type="int(11)")
        awake = Optional(int, sql_type="int(11)")
        cause_ac30 = Optional(int, sql_type="int(11)")
        cause_ac10 = Optional(int, sql_type="int(11)")

    return Caiyu_plat_push_week


if __name__ == "__main__":
    db = Database()
    tester = define_plat_push_week(db)
    print tester.__bases__
    print tester._table_