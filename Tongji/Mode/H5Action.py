# -*- coding: utf-8 -*-
from datetime import date
from pony.orm import *


def define_h5_action(db):

    class H5_action(db.Entity):
        id = PrimaryKey(int, sql_type="int(20)", auto=True)
        tm = Required(date)
        event = Required(str)
        pv = Optional(int, sql_type="int(11)")
        uv = Optional(int, sql_type="int(11)")

    return H5_action