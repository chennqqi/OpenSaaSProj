# -*- coding: utf-8 -*-
from datetime import date
from pony.orm import *


def define_plat_market_hy(db):

    class Plat_markey_hy(db.Entity):
        id = PrimaryKey(int, sql_type='int(20)', auto=True)
        tm = Required(date)
        ver = Required(str)
        pub = Required(str)
        newcomer = Optional(int)
        hy_0 = Optional(int)
        hy_1 = Optional(int)
        hy_2 = Optional(int)
        hy_3 = Optional(int)
        hy_4 = Optional(int)
        hy_5 = Optional(int)
        hy_6 = Optional(int)
        hy_7 = Optional(int)

    return Plat_markey_hy
