# -*- coding: utf-8 -*-
import __init__
from datetime import date
from pony.orm import *


def define_flightline_search_between(db):

    class Plat_flightline_search_between(db.Entity):
        id = PrimaryKey(int, sql_type="int(20)", auto=True)
        tm = Required(date)
        pub = Required(str)
        ver = Required(str)
        uv_all = Optional(int, sql_type="int(11)")
        pv_all = Optional(int, sql_type="int(11)")
        between_total = Optional(int, sql_type="int(11)")

        uv_1 = Optional(int, sql_type="int(11)", default=0)
        pv_1 = Optional(int, sql_type="int(11)", default=0)

        uv_2 = Optional(int, sql_type="int(11)", default=0)
        pv_2 = Optional(int, sql_type="int(11)", default=0)

        uv_3 = Optional(int, sql_type="int(11)", default=0)
        pv_3 = Optional(int, sql_type="int(11)", default=0)

        uv_4 = Optional(int, sql_type="int(11)", default=0)
        pv_4 = Optional(int, sql_type="int(11)", default=0)

        uv_5 = Optional(int, sql_type="int(11)", default=0)
        pv_5 = Optional(int, sql_type="int(11)", default=0)

        uv_6 = Optional(int, sql_type="int(11)", default=0)
        pv_6 = Optional(int, sql_type="int(11)", default=0)

    return Plat_flightline_search_between