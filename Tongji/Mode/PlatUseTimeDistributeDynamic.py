# -*- coding: utf-8 -*-
from datetime import date
from pony.orm import *


def define_plat_usetime_distribute_dynamic(db):

    class Plat_dur_distribute_dynamic(db.Entity):
        id = PrimaryKey(int, sql_type="int(20)", auto=True)
        tm = Required(date)
        ver = Required(str)
        pub = Required(str)
        uv_active = Optional(int, sql_type="int(11)")
        uv_dur = Optional(int, sql_type="int(11)")
        pv_dur = Optional(int, sql_type="int(11)")
        dur_total = Optional(int, sql_type="int(11)")
        partset = Optional(str)
        uv_part1 = Optional(int, sql_type="int(11)")
        uv_part2 = Optional(int, sql_type="int(11)")
        uv_part3 = Optional(int, sql_type="int(11)")
        uv_part4 = Optional(int, sql_type="int(11)")
        uv_part5 = Optional(int, sql_type="int(11)")

    return Plat_dur_distribute_dynamic


if __name__ == "__main__":
    pass