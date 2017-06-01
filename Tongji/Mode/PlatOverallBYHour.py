# -*- coding: utf-8 -*-
from datetime import datetime
from pony.orm import *


def define_overall_rt_byhour(db):

    class Plat_overall_rt_byhour(db.Entity):
        id = PrimaryKey(int, sql_type='int(20)', auto=True)
        tm = Required(datetime)
        plat = Optional(str)
        ver = Required(str)
        pub = Required(str)
        newcomer = Optional(int)
        addnewcomer = Optional(int)
        active = Optional(int)
        addactive = Optional(int)
        inpv = Optional(int)
        pagepv = Optional(int)

    return Plat_overall_rt_byhour