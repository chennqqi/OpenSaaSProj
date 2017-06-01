# -*- coding: utf-8 -*-
from datetime import date
from pony.orm import *


def define_plat_loc(db):

    class Plat_loc(db.Entity):
        id = PrimaryKey(int, sql_type='int(20)', auto=True)
        tm = Required(date)
        prov = Required(str)
        city = Required(str)
        ver = Required(str)
        pub = Required(str)
        newcomer = Optional(int)
        uv = Optional(int)
        alluser = Optional(int)
        intotal = Optional(int)
        pagetotal = Optional(int)

    return Plat_loc


if __name__ == "__main__":
    pass