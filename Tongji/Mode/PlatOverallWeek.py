# -*- coding: utf-8 -*-
from datetime import date
from pony.orm import *


def define_plat_overall_week(db):

    class Plat_overall_week(db.Entity):
        id = PrimaryKey(int, sql_type='int(20)', auto=True)
        tm = Required(date)
        ver = Required(str)
        pub = Required(str)
        newcomer = Optional(int)
        uv = Optional(int)
        sumuv = Optional(int)
        alluser = Optional(int)

    return Plat_overall_week