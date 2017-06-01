# -*- coding: utf-8 -*-
from datetime import date
from pony.orm import *


def define_h5_summary(db):

    class H5_summary(db.Entity):
        id = PrimaryKey(int, sql_type="int(20)", auto=True)
        tm = Required(date)
        prov = Optional(str)
        city = Optional(str)
        device = Optional(str)
        browser = Optional(str)
        uv = Optional(int, sql_type="int(11)")
        pagepv = Optional(int, sql_type="int(11)")
        pageuv = Optional(int, sql_type="int(11)")
        actionpv = Optional(int, sql_type="int(11)")
        actionuv = Optional(int, sql_type="int(11)")
        dur = Optional(int, sql_type="int(11)")
        durpv = Optional(int, sql_type="int(11)")
        duruv = Optional(int, sql_type="int(11)")

    return H5_summary





