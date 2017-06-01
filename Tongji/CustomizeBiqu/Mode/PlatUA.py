# -*- coding: utf-8 -*-
from Tongji.Mode.PlatUA import define_plat_ua
from pony.orm import *


def define_plat_ua_customize(db):

    class Plat_ua(define_plat_ua(db)):
        search_uv = Optional(int, sql_type="int(11)")
        search_pv = Optional(int, sql_type="int(11)")
        book_uv = Optional(int, sql_type="int(11)")
        book_pv = Optional(int, sql_type="int(11)")
        pay_uv = Optional(int, sql_type="int(11)")
        pay_pv = Optional(int, sql_type="int(11)")

    return Plat_ua

if __name__ == "__main__":
    db = Database()
    print(define_plat_ua_customize(db))