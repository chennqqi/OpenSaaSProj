# -*- coding: utf-8 -*-
import __init__
from pony.orm import *
# from SaaSConfig.config import mysql_host
# from SaaSConfig.config import mysql_port
# from SaaSConfig.config import mysql_user
# from SaaSConfig.config import mysql_passwd
from mapTablename import mapTablename
from DBClient import configPath
import ConfigParser

global mysql_host, mysql_port, mysql_user, mysql_passwd
cf = ConfigParser.ConfigParser()
cf.read(configPath)
mysql_host = cf.get("mysqldb", "mysql_host")
mysql_port = cf.getint("mysqldb", "mysql_port")
mysql_user = cf.get("mysqldb", "mysql_user")
mysql_passwd = cf.get("mysqldb", "mysql_passwd")

global _database
_database = {}


def bindTable(db, tablemode, dbname, tablename, host=mysql_host, port=mysql_port, user=mysql_user, passwd=mysql_passwd):
    # global _database
    mapTablename(tablemode, tablename)
    # db_key = "_".join([dbname, tablename])
    db.bind("mysql", host=host, port=port, user=user, passwd=passwd, db=dbname)
    db.generate_mapping(create_tables=True)
    return db

    # global _database
    # mapTablename(tablemode, tablename)
    # db_key = "_".join([dbname, tablename])
    # if db_key not in _database:
    #     _database[db_key] = db
    #     # mapTablename(tablemode, tablename)
    #     if not db.provider:
    #         db.bind("mysql", host=host, port=port, user=user, passwd=passwd, db=dbname)
    #         db.generate_mapping(create_tables=True)
    #     return _database[db_key]
    # elif db_key in _database:
    #     return _database[db_key]



