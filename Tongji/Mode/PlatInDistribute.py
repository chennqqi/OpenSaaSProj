# -*- coding: utf-8 -*-
from datetime import date
from pony.orm import *


def define_plat_in_distribute(db):
    class Plat_in_distribute(db.Entity):
        id = PrimaryKey(int, sql_type="int(20)", auto=True)
        tm = Required(date)
        ver = Required(str)
        pub = Required(str)
        uv_active = Optional(int, sql_type="int(11)")
        uv_in = Optional(int, sql_type="int(11)")
        pv_in = Optional(int, sql_type="int(11)")
        uv_1 = Optional(int, sql_type="int(11)", default=0)
        uv_2 = Optional(int, sql_type="int(11)", default=0)
        uv_3 = Optional(int, sql_type="int(11)", default=0)
        uv_4 = Optional(int, sql_type="int(11)", default=0)
        uv_5 = Optional(int, sql_type="int(11)", default=0)
        uv_6 = Optional(int, sql_type="int(11)", default=0)
        uv_7 = Optional(int, sql_type="int(11)", default=0)
        uv_8 = Optional(int, sql_type="int(11)", default=0)
        uv_9 = Optional(int, sql_type="int(11)", default=0)
        uv_10 = Optional(int, sql_type="int(11)", default=0)
        uv_gt10 = Optional(int, sql_type="int(11)", default=0)

    return Plat_in_distribute


if __name__ == "__main__":
    pass