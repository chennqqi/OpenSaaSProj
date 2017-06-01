# -*- coding: utf-8 -*-
from Tongji.Mode.PlatOS import define_plat_os
from pony.orm import *


def define_plat_os_customize(db):

    class Plat_os(define_plat_os(db)):
        search_uv = Optional(int, sql_type="int(11)")
        search_pv = Optional(int, sql_type="int(11)")
        book_uv = Optional(int, sql_type="int(11)")
        book_pv = Optional(int, sql_type="int(11)")
        pay_uv = Optional(int, sql_type="int(11)")
        pay_pv = Optional(int, sql_type="int(11)")

    return Plat_os

if __name__ == "__main__":
    db = Database()
    print(define_plat_os(db))