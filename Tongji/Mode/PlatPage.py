# -*- coding: utf-8 -*-
from datetime import date
from pony.orm import *


def define_plat_page(db):

    class Plat_page(db.Entity):
        id = PrimaryKey(int, sql_type='int(20)', auto=True)
        tm = Required(date)
        pageid = Required(str)
        ver = Required(str)
        pub = Required(str)
        uv = Optional(int)
        pv = Optional(int)

    return Plat_page