# -*- coding: utf-8 -*-
from datetime import date
from pony.orm import *


def define_h5_ref_source(db):

    class H5_ref_source(db.Entity):
        tm = Optional(date)
        hostname = Optional(str)
        refname = Optional(str)
        refid = Optional(str)
        totalpv = Optional(int, sql_type="int(11)")
        totaluv = Optional(int, sql_type="int(11)")
        ipuv = Optional(int, sql_type="int(11)")
        leaveuv = Optional(int, sql_type="int(11)")
        visitpv = Optional(int, sql_type="int(11)")
        dur = Optional(float)
        durpv = Optional(int, sql_type="int(11)")
        duruv = Optional(int, sql_type="int(11)")

    return H5_ref_source