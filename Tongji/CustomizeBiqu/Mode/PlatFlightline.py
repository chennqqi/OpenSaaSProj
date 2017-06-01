# -*- coding: utf-8 -*-
import __init__
from datetime import date
from pony.orm import *


def define_flightline(db):

    class Plat_flightline(db.Entity):
        id = PrimaryKey(int, sql_type="int(20)", auto=True)
        tm = Required(date)
        pub = Required(str)
        ver = Required(str)
        og = Required(str)
        dest = Required(str)
        singleuv = Optional(int, sql_type="int(11)")
        singlepv = Optional(int, sql_type="int(11)")
        rounduv = Optional(int, sql_type="int(11)")
        roundpv = Optional(int, sql_type="int(11)")
    return Plat_flightline