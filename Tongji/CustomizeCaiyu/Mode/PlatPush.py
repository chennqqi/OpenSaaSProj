# -*- coding: utf-8 -*-
from datetime import date
from pony.orm import *


def define_plat_push(db):

    class Caiyu_plat_push(db.Entity):
        tm = Optional(date)
        title = Optional(str, 2048)
        openpush = Optional(int, sql_type="int(11)")
        awake = Optional(int, sql_type="int(11)")
        cause_ac30 = Optional(int, sql_type="int(11)")
        cause_ac10 = Optional(int, sql_type="int(11)")

    return Caiyu_plat_push