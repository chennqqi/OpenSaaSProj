# -*- coding: utf-8 -*-
from Tongji.Mode.PlatOverall import define_plat_overall
from pony.orm import *


def define_plat_overall_customize(db):

    class Plat_overall(define_plat_overall(db)):
        search_uv = Optional(int, sql_type="int(11)")
        search_pv = Optional(int, sql_type="int(11)")
        book_uv = Optional(int, sql_type="int(11)")
        book_pv = Optional(int, sql_type="int(11)")
        pay_uv = Optional(int, sql_type="int(11)")
        pay_pv = Optional(int, sql_type="int(11)")

    return Plat_overall

if __name__ == "__main__":
    db = Database()
    print(define_plat_overall(db))