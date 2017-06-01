# -*- coding: utf-8 -*-
from datetime import date
from pony.orm import *


def define_plat_play_rank(db):

    class Plat_play_rank(db.Entity):
        id = PrimaryKey(int, sql_type='int(20)', auto=True)
        tm = Required(date)
        title = Optional(str)
        uv = Optional(int, sql_type='int(20)')
        pv = Optional(int, sql_type='int(20)')
        finishper = Optional(float)

    return Plat_play_rank

