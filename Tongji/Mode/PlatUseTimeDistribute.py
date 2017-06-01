# -*- coding: utf-8 -*-
from datetime import date
from pony.orm import *


def define_plat_usetime_distribute(db):

    class Plat_dur_distribute(db.Entity):
        id = PrimaryKey(int, sql_type="int(20)", auto=True)
        tm = Required(date)
        ver = Required(str)
        pub = Required(str)
        uv_active = Optional(int, sql_type="int(11)")
        uv_dur = Optional(int, sql_type="int(11)")
        pv_dur = Optional(int)
        dur_total = Optional(int, sql_type="int(11)")  # 总播放时长
        uv_lte1min = Optional(int, sql_type="int(11)", default=0)  # 表示人数
        uv_lte2min = Optional(int, sql_type="int(11)", default=0)
        uv_lte3min = Optional(int, sql_type="int(11)", default=0)
        uv_lte4min = Optional(int, sql_type="int(11)", default=0)
        uv_lte5min = Optional(int, sql_type="int(11)", default=0)
        uv_lte6min = Optional(int, sql_type="int(11)", default=0)
        uv_lte7min = Optional(int, sql_type="int(11)", default=0)
        uv_lte8min = Optional(int, sql_type="int(11)", default=0)
        uv_lte9min = Optional(int, sql_type="int(11)", default=0)
        uv_lte10min = Optional(int, sql_type="int(11)", default=0)
        uv_gt10min = Optional(int, sql_type="int(11)", default=0)

    return Plat_dur_distribute


if __name__ == "__main__":
    pass