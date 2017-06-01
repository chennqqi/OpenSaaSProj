# -*- coding: utf-8 -*-
from datetime import datetime
from pony.orm import *


def define_plat_event_id(db):

    class Plat_event_id(db.Entity):
        rowid = PrimaryKey(int, sql_type='int(20)', auto=True)
        inserttm = Optional(datetime)
        id = Optional(str)
        name = Optional(str)
        isdel = Optional(int, default=0)
        bz = Optional(str)

    return Plat_event_id


def define_plat_page_id(db):

    class Plat_page_id(db.Entity):
        rowid = PrimaryKey(int, auto=True)
        inserttm = Optional(datetime)
        id = Optional(str)
        name = Optional(str)
        isdel = Optional(int, default=0)
        bz = Optional(str)

    return Plat_page_id






