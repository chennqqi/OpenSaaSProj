# -*- coding: utf-8 -*-
# import time
import __init__
from MysqlClient import MysqlClient
from SaaSCommon.JHDecorator import fn_timer

@fn_timer
def update_d_appkey():
    client = MysqlClient("saas_server")
    con, cur = client.connection
    # sql_1 = '''SELECT a.appkey, a.plat, b.cdkey, a.enable*b.enable FROM (SELECT * FROM saas_meta.d_app) a LEFT JOIN (SELECT * FROM saas_meta.d_account) b on a.own = b.name_uid'''
    # sql_2 = '''SELECT a.appkey, a.plat, b.cdkey, b.enable FROM (SELECT * FROM customize.d_app) a LEFT JOIN (SELECT * FROM customize.d_account) b on a.own = b.name_uid'''
    # 合并 d_appkey
    # saas_server.d_appkey主键：appkey, plat, mongo_id, cdkey
    sql_replace = '''REPLACE INTO saas_server.d_appkey(inserttm, appkey, plat, mongo_id, cdkey, enable) \
SELECT NOW(), a.appkey, a.plat, \
CASE WHEN (c.mongo_id IS NULL OR c.mongo_id = '') THEN (SELECT MAX(id) FROM saas_server.d_mongo_server) ELSE c.mongo_id END AS mongo_id, b.cdkey, \
CASE WHEN c.enable IS NULL THEN a.enable*b.enable ELSE a.enable*b.enable*c.enable END as enable \
FROM (SELECT * FROM saas_meta.d_app WHERE enable = 1) a \
LEFT JOIN (SELECT * FROM saas_meta.d_account WHERE enable = 1) b on a.own = b.name_uid \
LEFT JOIN (SELECT * FROM saas_server.d_appkey) c ON a.appkey = c.appkey AND a.plat = c.plat AND c.cdkey = b.cdkey \
UNION \
SELECT NOW(), a.appkey, a.plat, \
CASE WHEN c.mongo_id IS NULL OR c.mongo_id = '' THEN (SELECT MAX(id) FROM saas_server.d_mongo_server) ELSE c.mongo_id END AS mongo_id, b.cdkey, \
CASE WHEN c.enable IS NULL THEN b.enable ELSE b.enable*c.enable END as enable \
FROM (SELECT * FROM customize.d_app) a \
LEFT JOIN (SELECT * FROM customize.d_account WHERE enable = 1) b on a.own = b.name_uid \
LEFT JOIN (SELECT * FROM saas_server.d_appkey) c ON a.appkey = c.appkey AND a.plat = c.plat AND c.cdkey = b.cdkey'''
    cur.execute(sql_replace)
    con.commit()
    client.closeMysql()

@fn_timer
def insert_d_log_collector(default_groupid=("JHSAASGroup_1",), execute_groupid=(6,)):
    # 获取新增加的appkey
    sql_appkeys = "SELECT appkey, plat FROM saas_server.d_appkey WHERE enable = 1 AND appkey NOT IN(SELECT DISTINCT(appkey) FROM saas_server.d_log_collector) AND plat IN ('android', 'ios', 'h5')"
    # 获取汇总的 group_id
    sql_groupids = "SELECT DISTINCT(group_id) FROM saas_server.d_log_collector"
    sql_insert_format = "INSERT INTO saas_server.d_log_collector (insert_tm , group_id, client_id, appkey, plat, logpath, enable) VALUES(NOW(), '%(group_id)s', '%(client_id)s', '%(appkey)s', '%(plat)s', '%(logpath)s', 1)"
    groupids = set()
    client = MysqlClient("saas_server")
    con, cur = client.connection
    cur.execute(sql_groupids)
    for item in cur.fetchall():
        groupid = item[0]
        groupids.add(groupid)
    cur.execute(sql_appkeys)
    for appkey, plat in [item for item in cur.fetchall()]:
        for groupid in groupids:
            if groupid in execute_groupid:
                continue
            sql_insert = sql_insert_format % {
                "group_id": groupid,
                "client_id": '',
                "appkey": appkey,
                "plat": plat,
                "logpath": plat,
            }
            print(sql_insert)
            cur.execute(sql_insert)
    con.commit()
    client.closeMysql()

def main():
    funcs = [update_d_appkey, insert_d_log_collector]
    for func in funcs:
        try:
            func()
        except:
            import traceback
            print(traceback.print_exc())

if __name__ == "__main__":
    import sys
    if "minute" in sys.argv:
        main()