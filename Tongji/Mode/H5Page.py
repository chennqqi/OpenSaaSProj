# -*- coding: utf-8 -*-
from datetime import date
from pony.orm import *


def define_h5_page(db):

    class H5_page(db.Entity):
        id = PrimaryKey(int, sql_type="int(20)", auto=True)
        tm = Required(date)
        refid = Optional(str)
        pageid = Required(str)
        pv = Optional(int, sql_type="int(11)")
        uv = Optional(int, sql_type="int(11)")
        dur = Optional(float)
        durpv = Optional(int, sql_type="int(11)")
        duruv = Optional(int, sql_type="int(11)")

    return H5_page

if __name__ == "__main__":
    db = Database()
    tester = define_h5_page(db)
    # print tester.__dict__["_root_"]
    print tester._root_ is tester