# -*- coding: utf-8 -*-
from datetime import datetime
from pony.orm import *


def define_plat_mapmeta(db):

    class Plat_mapmeta_udf(db.Entity):
        rowid = PrimaryKey(int, sql_type="int(20)", auto=True)
        inserttm = Optional(datetime)
        id = Required(str)  # eventid
        mapkey = Optional(str)
        keyname = Optional(str)
        valuetype = Optional(str)
        options = Optional(LongStr)
        relation = Optional(str)
        enable = Optional(int, default=1)
        
    return Plat_mapmeta_udf