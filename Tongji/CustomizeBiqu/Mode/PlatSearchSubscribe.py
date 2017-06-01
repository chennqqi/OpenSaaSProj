# -*- coding: utf-8 -*-
import __init__
from datetime import date
from pony.orm import *


def define_search_subscribe(db):

    class Plat_search_subscribe(db.Entity):
        id = PrimaryKey(int, sql_type="int(20)", auto=True)
        tm = Required(date)
        pub = Required(str)
        ver = Required(str)
        searchall = Optional(int, sql_type="int(11)")
        searchsingle = Optional(int, sql_type="int(11)")
        searchround = Optional(int, sql_type="int(11)")
        bookall = Optional(int, sql_type="int(11)")
        booksingle = Optional(int, sql_type="int(11)")
        bookround = Optional(int, sql_type="int(11)")
        searchsingle_booksingle = Optional(int, sql_type="int(11)")
        searchsingle_bookround = Optional(int, sql_type="int(11)")
        searchround_booksingle = Optional(int, sql_type="int(11)")
        searchround_bookround = Optional(int, sql_type="int(11)")

    return Plat_search_subscribe