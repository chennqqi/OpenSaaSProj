# -*- coding: utf-8 -*-
from datetime import date
from pony.orm import *


def define_plat_overall(db):

    class Plat_overall(db.Entity):
        id = PrimaryKey(int, sql_type='int(20)', auto=True)
        tm = Required(date)
        ver = Required(str)
        pub = Optional(str)
        newcomer = Optional(int, sql_type='int(20)')
        uv = Optional(int, sql_type='int(20)')
        playuv = Optional(int, sql_type='int(20)')  # ac15+ac18
        playpv = Optional(int, sql_type='int(20)')
        courseuv = Optional(int, sql_type='int(20)')  # ac18
        alluser = Optional(int, sql_type='int(20)')
        intotal = Optional(int, sql_type='int(20)')
        pagetotal = Optional(int, sql_type='int(20)')
        durtotal = Optional(int, sql_type='int(20)')

    return Plat_overall