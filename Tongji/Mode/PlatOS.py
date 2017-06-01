# -*- coding: utf-8 -*-
from datetime import date
from pony.orm import *


def define_plat_os(db):

    class Plat_os(db.Entity):
        id = PrimaryKey(int, sql_type='int(20)', auto=True)
        tm = Required(date)
        os = Required(str)
        ver = Required(str)
        pub = Required(str)
        newcomer = Optional(int)
        uv = Optional(int)
        alluser = Optional(int)
        intotal = Optional(int)
        pagetotal = Optional(int)

    return Plat_os
