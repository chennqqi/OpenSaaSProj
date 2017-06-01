# -*- coding: utf-8 -*-
from datetime import date
from pony.orm import *


def define_plat_overall(db):

    class Plat_overall(db.Entity):
        id = PrimaryKey(int, sql_type='int(20)', auto=True)
        tm = Required(date)
        ver = Required(str)
        pub = Optional(str)
        newcomer = Optional(int)
        uv = Optional(int)
        alluser = Optional(int)
        intotal = Optional(int)
        pagetotal = Optional(int)
        durtotal = Optional(int)

    return Plat_overall