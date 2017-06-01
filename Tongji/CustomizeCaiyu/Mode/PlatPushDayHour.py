# -*- coding: utf-8 -*-
from datetime import date
from pony.orm import *

def define_plat_push_day_hour(db):

    class Caiyu_plat_push_day_hour(db.Entity):
        id = PrimaryKey(int, sql_type="int(11)", auto=True)
        tm = Optional(date)
        hour_range = Optional(str)
        title = Optional(str, 2048)
        openpush = Optional(int, sql_type="int(11)")
        awake = Optional(int, sql_type="int(11)")
        cause_ac30 = Optional(int, sql_type="int(11)")
        cause_ac10 = Optional(int, sql_type="int(11)")

    return Caiyu_plat_push_day_hour
