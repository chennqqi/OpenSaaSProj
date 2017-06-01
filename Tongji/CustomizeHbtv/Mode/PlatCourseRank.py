# -*- coding: utf-8 -*-
from datetime import date
from pony.orm import *


def define_plat_course_rank(db):

    class Plat_course_rank(db.Entity):
        id = PrimaryKey(int, sql_type='int(20)', auto=True)
        tm = Required(date)
        title = Optional(str)
        uv = Optional(int, sql_type='int(20)')
        pv = Optional(int, sql_type='int(20)')
        ynumav = Optional(float)

    return Plat_course_rank

