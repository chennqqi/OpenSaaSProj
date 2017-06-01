# -*- coding: utf-8 -*-
from Tongji.Mode.PlatLoc import define_plat_loc
from pony.orm import *


def define_plat_loc_customize(db):

    class Plat_loc(define_plat_loc(db)):
        search_uv = Optional(int, sql_type="int(11)")
        search_pv = Optional(int, sql_type="int(11)")
        book_uv = Optional(int, sql_type="int(11)")
        book_pv = Optional(int, sql_type="int(11)")
        pay_uv = Optional(int, sql_type="int(11)")
        pay_pv = Optional(int, sql_type="int(11)")

    return Plat_loc



if __name__ == "__main__":
    db = Database()
    print(define_plat_loc_customize(db))