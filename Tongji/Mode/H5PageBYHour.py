# -*- coding: utf-8 -*-
from datetime import datetime
from pony.orm import *


def define_h5_page_rt_byhour(db):

    class H5_page_rt_byhour(db.Entity):
        id = PrimaryKey(int, sql_type="int(20)", auto=True)
        tm = Required(datetime)
        refid = Optional(str)
        pageid = Required(str)
        pv = Optional(int, sql_type="int(11)")
        uv = Optional(int, sql_type="int(11)")
        dur = Optional(int, sql_type="int(11)")
        durpv = Optional(int, sql_type="int(11)")
        duruv = Optional(int, sql_type="int(11)")

    return H5_page_rt_byhour