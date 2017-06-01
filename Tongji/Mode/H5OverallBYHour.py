# -*- coding: utf-8 -*-
from datetime import datetime
from pony.orm import *


def define_h5_overall_rt_byhour(db):

    class H5_overall_rt_byhour(db.Entity):
        id = PrimaryKey(int, sql_type="int(20)", auto=True)
        tm = Required(datetime)
        uv = Optional(int, sql_type="int(11)")
        pagepv = Optional(int, sql_type="int(11)")
        pageuv = Optional(int, sql_type="int(11)")
        actionpv = Optional(int, sql_type="int(11)")
        actionuv = Optional(int, sql_type="int(11)")

    return H5_overall_rt_byhour