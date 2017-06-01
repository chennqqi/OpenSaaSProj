# -*- coding: utf-8 -*-
from datetime import date
from pony.orm import *


def define_plat_market_remain(db):

    class Plat_market_remain(db.Entity):
        # _table_ = 'Plat_market_remain'
        id = PrimaryKey(int, sql_type='int(20)', auto=True)
        tm = Required(date)
        ver = Required(str)
        pub = Required(str)
        remain_0 = Optional(int)
        remain_1 = Optional(int)
        remain_3 = Optional(int)
        remain_7 = Optional(int)
        remain_15 = Optional(int)
        remain_30 = Optional(int)
        remain_90 = Optional(int)

    return Plat_market_remain