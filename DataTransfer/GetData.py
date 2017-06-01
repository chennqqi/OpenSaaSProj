# -*- coding: utf-8 -*-
import __init__
import datetime
import random
import time
import itertools

from DBClient.MysqlClient import MysqlClient


class GetData(object):

    def __init__(self):
        self.client = MysqlClient("saas_server")
        self.con, self.cur = self.client.connection
        self.start_day = "2017-05-15"

    def get_appkeys(self, append=None):
        result = {}
        sql = "select appkey, plat, cdkey from saas_server.d_appkey where enable = 1"
        self.cur.execute(sql)
        for item in self.cur.fetchall():
            appkey, plat, cdkey = item[0], item[1], item[2]
            result.setdefault(appkey, [plat, cdkey])
        if not (append is None):
            for key in append:
                result.setdefault(key, append[key])
        print '6666', result
        return result

    def get_dbs(self):
        data = self.get_appkeys()
        print data
        result = set()
        exclude_dbs = ["jh_48645aca02da76c6658b8d3e6d816b8e"]
        for key in data:
            if data[key][1] in exclude_dbs:
                continue
            result.add(data[key][1])
        result.add("mhmp")
        result.add("guagua")
        print result
        return result

    def clear_db(self):
        table_names_sql = "select table_name from information_schema.tables " \
                          "where table_schema='%(db_name)s' and table_type='base table'" % {"db_name": "produce"}

        drop_table_sql_format = "drop table if exists %(db_name)s.%(table_name)s"

        self.cur.execute(table_names_sql)
        for row in [row for row in self.cur.fetchall()]:
            table_name = row[0]
            if table_name.startswith("show_meidd_android_"):

                drop_table_sql = drop_table_sql_format % {
                    "table_name": table_name,
                    "db_name": "produce",
                }
                print drop_table_sql
                self.cur.execute(drop_table_sql)
        self.con.commit()
        self.client.closeMysql()

    def db_rsync_all(self, db_from, db_to="produce"):
        table_names_sql = "select table_name from information_schema.tables " \
                          "where table_schema='%(db_from)s' and table_type='base table'" % {"db_from": db_from}

        clear_sql_format = "drop table if exists %(db_to)s.%(table_name_to)s"

        execute_sql_format = "create table if not exists %(db_to)s.%(table_name_to)s as " \
                             "select * from %(db_from)s.%(table_name)s"

        sqls = []
        self.cur.execute(table_names_sql)
        for row in self.cur.fetchall():
            table_name = row[0]

            table_name_to = self.get_table_name_to(db_from, table_name)

            sqls.append(clear_sql_format % {"db_to": db_to, "table_name_to": table_name_to})
            execute_sql = execute_sql_format % {
                "db_from": db_from,
                "db_to": db_to,
                "table_name": table_name,
                "table_name_to": table_name_to
            }
            sqls.append(execute_sql)

        for sql in sqls:
            print sql
            self.cur.execute(sql)
        self.con.commit()
        # self.client.closeMysql()

    def test(self):
        max_tm_sql_format = "select max(%(colname_day)s) from %(db_from)s.%(table_name)s"
        max_tm_sql = max_tm_sql_format % {
            "colname_day": "tm",
            "db_from": "caiyu",
            "table_name": "caiyu_ad_android_overall",
        }
        self.cur.execute(max_tm_sql)
        for row in self.cur.fetchall():
            tm_max = row[0].strftime("%Y-%m-%d")
            print type(tm_max), tm_max

    def db_rsync_byday(self, num, db_from, db_to="produce"):

        create_table_sql_format = "create table if not exists %(db_to)s.%(table_name_to)s like %(db_from)s.%(table_name)s"

        table_names_sql_format = "select table_name from information_schema.tables " \
                                 "where table_schema='%(db_from)s' and table_type='base table'"

        clear_sql_format = "delete from %(db_to)s.%(table_name_to)s " \
                           "%(where)s"

        insert_sql_format = "insert into %(db_to)s.%(table_name_to)s " \
                            "select * from %(db_from)s.%(table_name)s " \
                            "%(where)s"

        where_format = "where %(colname_day)s between '%(day_start)s' and '%(day_end)s'"

        sqls = []
        self.cur.execute(table_names_sql_format % {
            "db_from": db_from
        })

        max_tm_sql_format = "select max(%(colname_day)s) from %(db_from)s.%(table_name)s"

        for row in [row for row in self.cur.fetchall()]:

            day = (datetime.datetime.now() - datetime.timedelta(days=num)).date().strftime("%Y-%m-%d")
            day_start = day + " " + "00:00:00"
            day_end = day + " " + "23:59:00"

            table_name = row[0]

            table_name_to = self.get_table_name_to(db_from, table_name)

            # 创建要插入的表
            create_table_sql = create_table_sql_format % {
                "db_from": db_from,
                "db_to": db_to,
                "table_name_to": table_name_to,
                "table_name": table_name
            }
            sqls.append(create_table_sql)
            colname = self.colname_date(db_from, table_name)

            if colname:
                max_tm_sql = max_tm_sql_format % {
                    "colname_day": colname,
                    "db_from": db_from,
                    "table_name": table_name,
                }
                self.cur.execute(max_tm_sql)
                for row in self.cur.fetchall():
                    if row[0]:
                        day_max = row[0].strftime("%Y-%m-%d")
                    else:
                        day_max = None
                if day_max is None:
                    continue
                elif day_max < day:
                    if table_name.endswith("_remain"):
                        day_start = (datetime.datetime.strptime(day_start, "%Y-%m-%d %H:%M:%S") - datetime.timedelta(days=91)).strftime("%Y-%m-%d %H:%M:%S")
                    else:
                        day_start = (datetime.datetime.strptime(day_start, "%Y-%m-%d %H:%M:%S") - datetime.timedelta(days=16)).strftime("%Y-%m-%d %H:%M:%S")
                elif day_max > day:
                    print table_name, table_name_to
                else:
                    print "*", colname, table_name, day_start, day_max, day
                _where = where_format % {
                    "colname_day": colname,
                    "day_start": day_start,
                    "day_end": day_end,
                }
            else:
                _where = ""
            # 删除重复数据
            clear_sql = clear_sql_format % {
                "db_to": db_to,
                "table_name_to": table_name_to,
                "where": _where,
            }
            sqls.append(clear_sql)
            # 插入数据
            insert_sql = insert_sql_format % {
                "db_from": db_from,
                "db_to": db_to,
                "table_name": table_name,
                "table_name_to": table_name_to,
                "where": _where
            }
            sqls.append(insert_sql)

        for sql in sqls:
            print sql
            try:
                self.cur.execute(sql)
            except:
                import traceback
                print traceback.print_exc()
        self.con.commit()
        # self.client.closeMysql()

    def colname_date(self, db_name, table_name):
        sql = "select column_name, data_type from information_schema.columns " \
              "where table_schema='%(db_name)s' and table_name='%(table_name)s' and data_type in ('datetime', 'date')" % {
                "db_name": db_name,
                "table_name": table_name,
        }
        self.cur.execute(sql)
        for item in self.cur.fetchall():
            return item[0]
        return None

    def db_tables(self, db_name):
        table_names_sql = "select table_name from information_schema.tables " \
                          "where table_schema='%(db_name)s' and table_type='base table'" % {"db_name": db_name}

        self.cur.execute(table_names_sql)

        tables = []
        for row in self.cur.fetchall():
            tables.append(row[0])
        return tables

    def db_rsync_byhour(self, db_from, num=0, db_to="produce"):

        day = (datetime.datetime.now() - datetime.timedelta(days=num)).date().strftime("%Y-%m-%d")
        day_start = day + " " + "00:00:00"
        day_end = day + " " + "23:59:00"

        create_table_sql_format = "create table if not exists %(db_to)s.%(table_name_to)s like %(db_from)s.%(table_name)s"

        table_names_sql_format = "select table_name from information_schema.tables " \
                                 "where table_schema='%(db_from)s' and table_type='base table'"

        clear_sql_format = "delete from %(db_to)s.%(table_name_to)s " \
                           "%(where)s"

        insert_sql_format = "insert into %(db_to)s.%(table_name_to)s " \
                            "select * from %(db_from)s.%(table_name)s " \
                            "%(where)s"

        where_format = "where %(colname_day)s between '%(day_start)s' and '%(day_end)s'"

        sqls = []
        self.cur.execute(table_names_sql_format % {
            "db_from": db_from
        })
        # 获取表名
        for row in [row for row in self.cur.fetchall()]:

            table_name = row[0]

            # 筛选出实时（小时）表
            if not ("hour" in table_name):
                continue

            table_name_to = self.get_table_name_to(db_from, table_name)

            # 创建要插入的表
            create_table_sql = create_table_sql_format % {
                "db_from": db_from,
                "db_to": db_to,
                "table_name_to": table_name_to,
                "table_name": table_name
            }
            sqls.append(create_table_sql)
            colname = self.colname_date(db_from, table_name)

            if colname:
                _where = where_format % {
                    "colname_day": colname,
                    "day_start": day_start,
                    "day_end": day_end,
                }
            else:
                _where = ""
                continue
            # 删除重复数据
            clear_sql = clear_sql_format % {
                "db_to": db_to,
                "table_name_to": table_name_to,
                "where": _where,
            }
            sqls.append(clear_sql)
            # 插入数据
            insert_sql = insert_sql_format % {
                "db_from": db_from,
                "db_to": db_to,
                "table_name": table_name,
                "table_name_to": table_name_to,
                "where": _where
            }
            sqls.append(insert_sql)

        for sql in sqls:
            print sql
            self.cur.execute(sql)
        self.con.commit()
        # self.client.closeMysql()

    def data_regression(self):
        appkeys = set()
        sql = "select appkey from saas_server.d_appkey where enable = 1"
        self.cur.execute(sql)
        for row in self.cur.fetchall():
            appkey = row[0]
            if appkey not in ['caiyu_ad', 'caiyu_ad_bankext', 'caiyu_ad_housefundext', 'caiyu_ios_bankassist', 'caiyu_ios_bee', 'caiyu_ios_free', 'caiyu_ios_fubao', 'caiyu_ios']:
                continue
            appkeys.add("_".join(["show", appkey]))

        tables = self.db_tables("produce")
        print tables
        # table_index = tables.index("show_feeling_ios_loc")

        for table in tables[:]:

            try:

                if not any(map(lambda item: table.startswith(item + "_"), appkeys)):
                    continue

                colname_day = self.colname_date("produce", table)

                if not colname_day:
                    print table
                    continue

                min_max_tm_sql = "select min(%(colname_day)s), max(%(colname_day)s) from %(db_name)s.%(table_name)s" % {
                    "colname_day": colname_day,
                    "db_name": "produce",
                    "table_name": table,
                }

                # 获取最大日期跨度
                self.cur.execute(min_max_tm_sql)
                for row in self.cur.fetchall():
                    min_tm, max_tm = row[0], row[1]

                for appkey in appkeys:
                    if table.startswith(appkey + "_"):
                        appkey = appkey
                        plat, table_tag = table.split(appkey + "_")[1].split("_")[0], "_".join(table.split(appkey + "_")[1].split("_")[1:])
                        break

                while min_tm <= max_tm:
                    if type(max_tm) == type(datetime.datetime.now()):
                        num = (datetime.datetime.now() - max_tm).days
                    elif type(max_tm) == type(datetime.datetime.now().date()):
                        num = (datetime.datetime.now().date() - max_tm).days
                    else:
                        print max_tm, type(max_tm)
                        break
                    self.table_data_hanlder(num, appkey, plat, table_tag, max_tm)
                    max_tm += datetime.timedelta(days=-1)
            except:
                import traceback
                print traceback.print_exc()

    def data_regression_daily(self, num):

        appkeys = set()
        sql = "select appkey from saas_server.d_appkey where enable = 1"
        self.cur.execute(sql)
        for row in self.cur.fetchall():
            appkey = row[0]
            if appkey not in ['caiyu_ad', 'caiyu_ad_bankext', 'caiyu_ad_housefundext', 'caiyu_ios_bankassist', 'caiyu_ios_bee', 'caiyu_ios_free', 'caiyu_ios_fubao', 'caiyu_ios']:
                continue
            appkeys.add("_".join(["show", appkey]))

        print appkeys

        tables = self.db_tables("produce")
        print tables
        table_index = tables.index("show_feeling_ios_loc")


        for table in tables[:]:

            if any(map(lambda item: table.endswith(item), ["_board", "_board_module"])):
                continue

            try:
                if not any(map(lambda item: table.startswith(item + "_"), appkeys)):
                    continue

                colname_day = self.colname_date("produce", table)

                if not colname_day:
                    print table
                    continue

                min_max_tm_sql = "select min(%(colname_day)s), max(%(colname_day)s) from %(db_name)s.%(table_name)s" % {
                    "colname_day": colname_day,
                    "db_name": "produce",
                    "table_name": table,
                }

                # 获取最大日期跨度
                self.cur.execute(min_max_tm_sql)
                for row in self.cur.fetchall():
                    min_tm, max_tm = row[0], row[1]
                    if min_tm is None or max_tm is None:
                        break
                    max_tm = max_tm - datetime.timedelta(days=num-1)

                min_tm = max_tm

                if min_tm is None or max_tm is None:
                    print table
                    continue

                for appkey in appkeys:
                    if table.startswith(appkey + "_"):
                        appkey = appkey
                        plat, table_tag = table.split(appkey + "_")[1].split("_")[0], "_".join(table.split(appkey + "_")[1].split("_")[1:])
                        break

                while min_tm <= max_tm:
                    if type(max_tm) == type(datetime.datetime.now()):
                        num = (datetime.datetime.now() - max_tm).days
                    elif type(max_tm) == type(datetime.datetime.now().date()):
                        num = (datetime.datetime.now().date() - max_tm).days
                    else:
                        print max_tm, type(max_tm)
                        break
                    self.table_data_hanlder(num, appkey, plat, table_tag, max_tm)
                    max_tm += datetime.timedelta(days=-1)
            except:
                import traceback
                print traceback.print_exc()


    def large_data_daily(self, num, large=1.3, appkey_s = ('walkup',), plat_table_tag = None):
        appkeys = set()
        sql = "select appkey from saas_server.d_appkey where enable = 1"
        self.cur.execute(sql)
        append = {
            "walkup": ["ios", "."],
            "meidd": ["ios", "."],
            "gouqs": ["ios", "."],
        }
        for row in self.get_appkeys(append=append).keys():

            appkey = row
            if appkey not in appkey_s:
                continue
            appkeys.add("_".join(["show", appkey]))

        print '777', appkeys

        tables = self.db_tables("produce")

        for table in tables[:]:

            if any(map(lambda item: table.endswith(item), ["_board", "_board_module"])):
                continue

            try:
                if not any(map(lambda item: table.startswith(item + "_"), appkeys)):
                    continue

                colname_day = self.colname_date("produce", table)

                if not colname_day:
                    print table
                    continue

                min_max_tm_sql = "select min(%(colname_day)s), max(%(colname_day)s) from %(db_name)s.%(table_name)s" % {
                    "colname_day": colname_day,
                    "db_name": "produce",
                    "table_name": table,
                }

                # 获取最大日期跨度

                self.cur.execute(min_max_tm_sql)
                for row in self.cur.fetchall():
                    min_tm, max_tm = row[0], row[1]

                    if min_tm is None or max_tm is None:
                        break
                    max_tm = max_tm - datetime.timedelta(days=num-1)
                    min_tm = max_tm

                if min_tm is None or max_tm is None:
                    continue

                for appkey in appkeys:
                    if table.startswith(appkey + "_"):
                        appkey = appkey
                        plat, table_tag = table.split(appkey + "_")[1].split("_")[0], "_".join(table.split(appkey + "_")[1].split("_")[1:])
                        break

                while min_tm <= max_tm:
                    if type(max_tm) == type(datetime.datetime.now()):
                        _num = (datetime.datetime.now() - max_tm).days
                    elif type(max_tm) == type(datetime.datetime.now().date()):
                        _num = (datetime.datetime.now().date() - max_tm).days
                    else:
                        break
                    self.table_data_hanlder(_num, appkey, plat, table_tag, max_tm, setper=large, plat_table_tag=plat_table_tag)
                    max_tm += datetime.timedelta(days=-1)
            except:
                import traceback
                print traceback.print_exc()

    def table_data_hanlder(self, num, appkey, plat, table_tag, max_tm, db_to = "produce", setper = None, plat_table_tag = None):

        table_tags_table = {
            ("ios", "overall"): [0, 0, "uv", "intotal", "pagetotal", "durtotal"],
            ("android", "overall"): [0, 0, "uv", "intotal", "pagetotal", "durtotal"],
            ("ios", "event"): [0, 0, "uv", "pv"],
            ("android", "event"): [0, 0, "uv", "pv"],
            ("ios", "loc"): [0, 0, "uv", "intotal", "pagetotal"],
            ("android", "loc"): [0, 0, "uv", "intotal", "pagetotal"],
            ("ios", "os"): [0, 0, "uv", "intotal", "pagetotal"],
            ("android", "os"): [0, 0, "uv", "intotal", "pagetotal"],
            ("ios", "overall_rt_byhour"): [0, 0, "active", "addactive", "inpv", "pagepv"],
            ("android", "overall_rt_byhour"): [0, 0, "active", "addactive", "inpv", "pagepv"],
            ("ios", "page"): [0, 0, "uv", "pv"],
            ("android", "page"): [0, 0, "uv", "pv"],
            ("ios", "ua"): [0, 0, "uv", "intotal", "pagetotal"],
            ("android", "ua"): [0, 0, "uv", "intotal", "pagetotal"],
            ("ios", "newuser_events"): [0, 0, "uv_1", "uv_2", "uv_3", "uv_4", "uv_5", "uv_6", "uv_7", "uv_8"],
            ("android", "newuser_events"): [0, 0, "uv_1", "uv_2", "uv_3", "uv_4", "uv_5", "uv_6", "uv_7", "uv_8"],
            # ("ios", "g_type"): [1, 16, "remain_1", "remain_3", "remain_7", "remain_15", "day_7", "day_7_ac11", "day_7_ac17", "day_7_ac10"],
            # ("android", "g_type"): [1, 16, "remain_1", "remain_3", "remain_7", "remain_15", "day_7", "day_7_ac11", "day_7_ac17", "day_7_ac10"],
            # ("ios", "market_hy_7"): [7, 0, "newcomer", "hy_0", "hy_1", "hy_2", "hy_3", "hy_4", "hy_5", "hy_6", "hy_7"],
            # ("android", "market_hy_7"): [7, 0, "newcomer", "hy_0", "hy_1", "hy_2", "hy_3", "hy_4", "hy_5", "hy_6", "hy_7"],
            # ("ios", "market_overall"): [0, 0, "newcomer", "intotal", "pagetotal", "durtotal"],
            # ("android", "market_overall"): [0, 0, "newcomer", "intotal", "pagetotal", "durtotal"],
            ("ios", "overall_week"): [0, 0, "uv", "sumuv"],
            ("android", "overall_week"): [0, 0, "uv", "sumuv"],
            ("ios", "in_distribute"): [0, 0, "uv_active", "uv_in", "pv_in", "uv_1", "uv_2", "uv_3", "uv_4", "uv_5", "uv_6", "uv_7", "uv_8", "uv_9", "uv_10", "uv_gt10"],
            ("android", "in_distribute"): [0, 0, "uv_active", "uv_in", "pv_in", "uv_1", "uv_2", "uv_3", "uv_4", "uv_5", "uv_6", "uv_7", "uv_8", "uv_9", "uv_10", "uv_gt10"],


            ("h5", "event"): [0, 0, "pv", "uv"],
            ("h5", "event_rt_byhour"): [0, 0, "pv", "uv"],
            ("h5", "overall"): [0, 0, "uv", "pagepv", "pageuv", "actionpv", "actionuv"],
            ("h5", "overall_rt_byhour"): [0, 0, "uv", "pagepv", "pageuv", "actionpv", "actionuv"],
            ("h5", "page"): [0, 0, "pv", "uv", "dur", "durpv", "duruv"],
            ("h5", "page_rt_byhour"): [0, 0, "pv", "uv", "dur", "durpv", "duruv"],
            ("h5", "ref_source"): [0, 0, "totalpv", "totaluv", "ipuv", "leaveuv", "visitpv", "dur", "durpv", "duruv"],
            ("h5", "summary"): [0, 0, "uv", "pagepv", "pageuv", "actionpv", "actionuv", "dur", "durpv", "duruv"],
        }

        if plat_table_tag is None:
            table_tags_table = table_tags_table
        else:
            tmp = {}
            [tmp.setdefault(item, table_tags_table[item]) for item in plat_table_tag if item in table_tags_table]
            table_tags_table = tmp


        cur_date = datetime.datetime.now().date() - datetime.timedelta(days=num)

        if (plat, table_tag) not in table_tags_table:
            return

        cols = table_tags_table[(plat, table_tag)]

        if type(max_tm) == type(datetime.datetime.now()):
            max_tm = max_tm.date()

        if max_tm < (cur_date - datetime.timedelta(days=cols[0])):
            print num, appkey, plat, table_tag, "maxtm: ", max_tm, cur_date - datetime.timedelta(days=cols[0])
            return

        update_num = cols[1]
        for _num in range(0, update_num+1):
            if setper is None:
                self.seed = ((datetime.datetime.now()-datetime.timedelta(days=num+_num)).date() - datetime.datetime.strptime("2017-05-16", "%Y-%m-%d").date()).days
                per = self.seed/10.0

                # 每十天变化4%
                if per >= 0:
                    per = 0
                else:
                    per = per * 0.04
                    if per <= -0.8:
                        per = -0.8
            else:
                per = setper

            sql_format = "update %(db_to)s.%(table_name)s set %(set_cols)s where tm = '%(tm)s'"

            set_format = "%(col_name)s = case when %(col_name)s*(1%(expand)s) > 1 then %(col_name)s*(1%(expand)s) else 1 end"

            set_cols = []

            for col_name in cols[2:]:
                tm = (datetime.datetime.now() - datetime.timedelta(days=num+cols[0])).date().strftime("%Y-%m-%d")
                set_item = set_format % {"col_name": col_name, "expand": ("+" if per >= 0 else "") + str(per),}
                set_cols.append(set_item)
            set_cols_key = ", ".join(set_cols)

            sql = sql_format % {
                "db_to": db_to,
                "table_name": "_".join([appkey, plat, table_tag]),
                "set_cols": set_cols_key,
                "tm": tm,
            }

            print sql
            self.cur.execute(sql)
            self.con.commit()

    def create_data(self, start_day, end_day, seed_start_day, seed_end_day, db_from, db_to="produce"):

        start_date = datetime.datetime.strptime(start_day, "%Y-%m-%d")
        end_date = datetime.datetime.strptime(end_day, "%Y-%m-%d")
        seed_start_date = datetime.datetime.strptime(seed_start_day, "%Y-%m-%d")
        seed_end_date = datetime.datetime.strptime(seed_end_day, "%Y-%m-%d")
        choice_days = []

        while seed_start_date <= seed_end_date:
            choice_days.append(seed_start_date.strftime("%Y-%m-%d"))
            seed_start_date += datetime.timedelta(days=1)

        table_names_sql = "select table_name from information_schema.tables " \
                          "where table_schema='%(db_from)s' and table_type='base table'" % {"db_from": db_from}
        self.cur.execute(table_names_sql)

        table_names_from_to = []
        for row in self.cur.fetchall():
            table_name = row[0]
            if table_name.startswith("ncf_h5"):
            # if table_name not in ["ncf_360_h5_overall"]:
                continue
            table_names_from_to.append((table_name, self.get_table_name_to(db_from, table_name)))

        clear_sql_format = "delete from %(db_to)s.%(table_name_to)s where %(colname_day)s between '%(day_start)s' and '%(day_end)s'"

        create_sql_format = "create table if not exists %(db_to)s.%(table_name_to)s like %(db_from)s.%(table_name)s"

        insert_sql_format = "insert into %(db_to)s.%(table_name_to)s (%(col_names)s) " \
                            "select %(col_names_selected)s from %(db_from)s.%(table_name)s " \
                            "where %(colname_day)s between '%(day_start)s' and '%(day_end)s'"

        sqls = []

        while start_date <= end_date:
            day_selected = random.choice(choice_days)
            day_delta = (start_date - datetime.datetime.strptime(day_selected, "%Y-%m-%d")).days

            day_start = start_date.strftime("%Y-%m-%d") + " 00:00:00"
            day_end = start_date.strftime("%Y-%m-%d") + " 23:59:00"

            for table_name, table_name_to in table_names_from_to:
                col_name_date = self.colname_date(db_from, table_name)

                if not col_name_date:
                    continue

                create_sql = create_sql_format % {
                    "db_to": db_to,
                    "table_name_to": table_name_to,
                    "db_from": db_from,
                    "table_name": table_name,
                }
                sqls.append(create_sql)
                clear_sql = clear_sql_format % {
                    "db_to": db_to,
                    "table_name_to": table_name_to,
                    "colname_day": col_name_date,
                    "day_start": day_start,
                    "day_end": day_end,
                }
                sqls.append(clear_sql)

                col_names = self.get_table_columns(db_from, table_name)

                col_names_str = ""
                for index, col in enumerate(col_names):
                    col_names_str += ("`" + col + "`")
                    if index < len(col_names) - 1:
                        col_names_str += ", "

                col_names_selected_str = ""
                for index, col in enumerate(col_names):
                    if col != col_name_date:
                        col_names_selected_str += ("`" + col + "`")
                    else:
                        col_names_selected_str += ("date_add(" + col + ", " + "interval " + (str(day_delta)) + " day)")
                    if index < len(col_names) - 1:
                        col_names_selected_str += ", "

                insert_sql = insert_sql_format % {
                    "db_to": db_to,
                    "db_from": db_from,
                    "table_name": table_name,
                    "table_name_to": table_name_to,
                    "col_names": col_names_str,
                    "col_names_selected": col_names_selected_str,
                    "colname_day": col_name_date,
                    "day_start": day_selected + " 00:00:00",
                    "day_end": day_selected + " 23:59:00",
                }
                sqls.append(insert_sql)
            start_date += datetime.timedelta(days=1)

        for index, sql in enumerate(sqls):

            try:
                print index, sql
                self.cur.execute(sql)
                self.con.commit()
            except:
                import traceback
                print traceback.print_exc()
        # self.client.closeMysql()

    def create_data_appkeyA_appkeyB(self, start_day, end_day, seed_start_day, seed_end_day, db_from, appkey_from, appkey_to, plat_from, plat_to, db_to="produce"):

        start_date = datetime.datetime.strptime(start_day, "%Y-%m-%d")
        end_date = datetime.datetime.strptime(end_day, "%Y-%m-%d")
        seed_start_date = datetime.datetime.strptime(seed_start_day, "%Y-%m-%d")
        seed_end_date = datetime.datetime.strptime(seed_end_day, "%Y-%m-%d")
        choice_days = []

        table_head_from = "%(appkey)s_%(plat)s_" % {
            "appkey": appkey_from,
            "plat": plat_from,
        }

        table_head_to = "show_%(appkey)s_%(plat)s_" % {
            "appkey": appkey_to,
            "plat": plat_to,
        }

        while seed_start_date <= seed_end_date:
            choice_days.append(seed_start_date.strftime("%Y-%m-%d"))
            seed_start_date += datetime.timedelta(days=1)

        table_names_sql_format = "select table_name from information_schema.tables " \
                          "where table_schema='%(db_name)s' and table_name like '%(table_head)s%%' and table_type='base table'"

        table_names_sql_from = table_names_sql_format % {"db_name": db_from, "table_head": table_head_from}

        self.cur.execute(table_names_sql_from)

        table_names_from_to = []
        for row in self.cur.fetchall():
            table_name_from = row[0]
            table_tag = table_name_from.split(table_head_from)[1]
            table_name_to = "".join([table_head_to, table_tag])
            table_names_from_to.append((table_name_from, table_name_to))

        clear_sql_format = "delete from %(db_to)s.%(table_name_to)s where %(colname_day)s between '%(day_start)s' and '%(day_end)s'"

        create_sql_format = "create table if not exists %(db_to)s.%(table_name_to)s like %(db_from)s.%(table_name)s"

        insert_sql_format = "insert into %(db_to)s.%(table_name_to)s (%(col_names)s) " \
                            "select %(col_names_selected)s from %(db_from)s.%(table_name)s " \
                            "where %(colname_day)s between '%(day_start)s' and '%(day_end)s'"

        sqls = []

        while start_date <= end_date:
            day_selected = random.choice(choice_days)
            day_delta = (start_date - datetime.datetime.strptime(day_selected, "%Y-%m-%d")).days

            day_start = start_date.strftime("%Y-%m-%d") + " 00:00:00"
            day_end = start_date.strftime("%Y-%m-%d") + " 23:59:00"

            for table_name, table_name_to in table_names_from_to:
                # 排除定义表
                if "_udf" in table_name:
                    continue
                if "_board" in table_name:
                    continue
                col_name_date = self.colname_date(db_from, table_name)

                if not col_name_date:
                    continue

                create_sql = create_sql_format % {
                    "db_to": db_to,
                    "table_name_to": table_name_to,
                    "db_from": db_from,
                    "table_name": table_name,
                }
                sqls.append(create_sql)
                clear_sql = clear_sql_format % {
                    "db_to": db_to,
                    "table_name_to": table_name_to,
                    "colname_day": col_name_date,
                    "day_start": day_start,
                    "day_end": day_end,
                }
                sqls.append(clear_sql)

                col_names = self.get_table_columns(db_from, table_name)
                # col_names_str = ", ".join(map(lambda item: item if item not in ["desc", "enable"] else "`" + item + "`", col_names))

                col_names_str = ""
                for index, col in enumerate(col_names):
                    if col in ["id"]:
                        continue
                    col_names_str += ("`" + col + "`")
                    if index < len(col_names) - 1:
                        col_names_str += ", "

                col_names_selected_str = ""
                for index, col in enumerate(col_names):
                    if col in ["id"]:
                        continue
                    if col != col_name_date:
                        col_names_selected_str += ("`" + col + "`")
                    else:
                        col_names_selected_str += ("date_add(" + col + ", " + "interval " + (str(day_delta)) + " day)")
                    if index < len(col_names) - 1:
                        col_names_selected_str += ", "

                insert_sql = insert_sql_format % {
                    "db_to": db_to,
                    "db_from": db_from,
                    "table_name": table_name,
                    "table_name_to": table_name_to,
                    "col_names": col_names_str,
                    "col_names_selected": col_names_selected_str,
                    "colname_day": col_name_date,
                    "day_start": day_selected + " 00:00:00",
                    "day_end": day_selected + " 23:59:00",
                }
                sqls.append(insert_sql)
            start_date += datetime.timedelta(days=1)

        for index, sql in enumerate(sqls):
            try:
                self.cur.execute(sql)
                self.con.commit()
                print index, sql
            except:
                import traceback
                print traceback.print_exc()
                print "*", index, sql

        # self.client.closeMysql()


    def get_table_columns(self, db_name, table_name):
        sql = "select column_name from information_schema.columns where table_schema='%(db_name)s' and table_name='%(table_name)s'" % {
            "db_name": db_name,
            "table_name": table_name
        }
        self.cur.execute(sql)
        col_names = []
        for row in self.cur.fetchall():
            col = row[0]
            col_names.append(col)
        return col_names


    def get_table_name_to(self, db_from, table_name):
        if db_from in ["guaeng"]:
            if not any(map(lambda item: table_name.startswith(item), ["huiyue", "guaeng", "hbtv", "mhmp"])):
                table_name_to = "_".join(["show", db_from, "ios", table_name])
            else:
                table_name_to = "_".join(["show", table_name])
        elif db_from in ["mhmp", "guagua"]:
            table_name_to = "_".join(["show", db_from, "ios", table_name])
        else:
            table_name_to = "_".join(["show", table_name])
        return table_name_to

    def update_overall(self, db_name, table_name, min_tm = None, max_tm = None, colname_day="tm"):

        max_tm_sql = "select min(%(colname_day)s), max(%(colname_day)s) from %(db_name)s.%(table_name)s" % {
            "colname_day": colname_day,
            "db_name": db_name,
            "table_name": table_name,
        }
        print max_tm_sql
        self.cur.execute(max_tm_sql)

        for item in self.cur.fetchall():
            min_tm = item[0] if min_tm is None else datetime.datetime.strptime(min_tm, "%Y-%m-%d").date()
            max_tm = item[1] if max_tm is None else datetime.datetime.strptime(max_tm, "%Y-%m-%d").date()

        select_sql_format = "select ver, pub, newcomer, alluser from %(db_name)s.%(table_name)s where tm = '%(tm)s'"

        update_sql_format = "update %(db_name)s.%(table_name)s set alluser = %(alluser)d + newcomer where pub = '%(pub)s' and ver = '%(ver)s' and tm = '%(tm)s'"

        while min_tm < max_tm:
            min_tm += datetime.timedelta(days=1)
            select_sql = select_sql_format % {
                "db_name": db_name,
                "table_name": table_name,
                "tm": (min_tm - datetime.timedelta(days=1)).strftime("%Y-%m-%d"),
            }
            self.cur.execute(select_sql)
            sqls = []
            for item in self.cur:
                ver, pub, newcomer, alluser = item
                update_sql = update_sql_format % {
                    "db_name": db_name,
                    "table_name": table_name,
                    "alluser": alluser,
                    "pub": pub,
                    "ver": ver,
                    "tm": min_tm.strftime("%Y-%m-%d"),
                }
                sqls.append(update_sql)

            for index, sql in enumerate(sqls):
                try:
                    self.cur.execute(sql)
                    if "pub = 'all' and ver = 'all'" in sql:
                        print index, sql
                except:
                    import traceback
                    print traceback.print_exc()
                    print "*", index, sql
        self.con.commit()

    def move_table(self, db_from, table_from, table_to, db_to="produce"):

        sqls = []

        clear_sql = "drop table if exists %(db_to)s.%(table_to)s" % {
            "db_to": db_to,
            "table_to": table_to
        }

        sqls.append(clear_sql)

        execute_sql = "create table if not exists %(db_to)s.%(table_to)s as " \
                             "select * from %(db_from)s.%(table_from)s" % {
            "db_to": db_to,
            "table_to": table_to,
            "db_from": db_from,
            "table_from": table_from
        }
        sqls.append(execute_sql)

        for sql in sqls:
            print sql
            self.cur.execute(sql)
        self.con.commit()





if __name__ == "__main__":

    import sys

    if "test" in sys.argv:
        num = 1
        start_day = (datetime.datetime.now() - datetime.timedelta(days=num)).strftime("%Y-%m-%d")
        end_day = start_day
        worker = GetData()
        # worker.create_data_appkeyA_appkeyB(start_day='2017-05-22', end_day='2017-05-22', seed_start_day='2016-08-25',
        #                                    seed_end_day='2016-11-10', db_from="guaeng", appkey_from="hbtv",
        #                                    appkey_to="meidd", plat_from="android", plat_to='ios', db_to="produce")
        worker.move_table(db_from="ncf", table_from="ncf_ws_h5_page_rt_byhour", table_to="show_ncf_ws_h5_page_rt_byhour", db_to="produce")
        worker.move_table(db_from="ncf", table_from="ncf_h5_h5_page_rt_byhour", table_to="show_ncf_h5_h5_page_rt_byhour", db_to="produce")
        worker.move_table(db_from="ncf", table_from="ncf_360_h5_page_rt_byhour", table_to="show_ncf_360_h5_page_rt_byhour", db_to="produce")
        # worker.move_table(db_from="guaeng", table_from="hbtv_android_event", table_to="show_meidd_ios_event", db_to="produce")
        # worker.move_table(db_from="guaeng", table_from="hbtv_android_page", table_to="show_meidd_ios_page", db_to="produce")
        # worker.move_table(db_from="guaeng", table_from="hbtv_android_loc", table_to="show_jkp_ios_loc", db_to="produce")
        # worker.large_data_daily(1, large=-0.6, plat_table_tag=[('ios', 'event')])
        # worker.move_table(db_from="guaeng", table_from="hbtv_android_loc", table_to="show_meidd_ios_loc", db_to="produce")
        # # 伪造 够轻松 1000~，2016-06-30
        # worker.move_table(db_from="caiyu", table_from="caiyu_ios_free_ios_overall_rt_byhour", table_to="show_gouqs_ios_overall_rt_byhour", db_to="produce")
        # worker.move_table(db_from="caiyu", table_from="caiyu_ios_free_ios_event_rt_byhour", table_to="show_gouqs_ios_event_rt_byhour", db_to="produce")
        # # 伪造 walkup 5000~，2016-05-25
        # worker.move_table(db_from="caiyu", table_from="caiyu_ios_free_ios_overall_rt_byhour", table_to="show_walkup_ios_overall_rt_byhour", db_to="produce")
        # worker.move_table(db_from="caiyu", table_from="caiyu_ios_free_ios_event_rt_byhour", table_to="show_walkup_ios_event_rt_byhour", db_to="produce")
        # 伪造 美滴滴 300~500，2016-09-28
        # worker.move_table(db_from="guaeng", table_from="huiyue_ios_ios_overall_rt_byhour", table_to="show_meidd_ios_overall_rt_byhour", db_to="produce")
        # worker.move_table(db_from="guaeng", table_from="huiyue_ios_ios_event_rt_byhour", table_to="show_meidd_ios_event_rt_byhour", db_to="produce")
        # worker.clear_db()
        # print worker.get_dbs()
        # start_date = datetime.datetime.strptime("2016-05-31", "%Y-%m-%d").date()
        # end_date = datetime.datetime.strptime("2017-05-20", "%Y-%m-%d").date()
        # while start_date <= end_date:
        #     num = (datetime.datetime.now().date() - start_date).days
        #     worker.large_data_daily(num)
        #     start_date += datetime.timedelta(days=1)
        # worker.move_table(db_from="jh_10a0e81221095bdba91f7688941948a6", table_from="biqu_all_all_market_remain", table_to="show_jiayu_ios_ios_market_remain", db_to="produce")
        # worker.move_table(db_from="jh_10a0e81221095bdba91f7688941948a6", table_from="biqu_all_all_market_remain", table_to="show_jkp_ios_ios_market_remain", db_to="produce")
        # print worker.large_data_daily(1, 4)

        # worker.move_table(db_from="feeling", table_from="feeling_ios_overall_rt_byhour", table_to="show_feeling_ios_overall_rt_byhour", db_to="produce")
        # worker.move_table(db_from="feeling", table_from="feeling_ios_event_rt_byhour", table_to="show_feeling_ios_event_rt_byhour", db_to="produce")
        # worker.move_table(db_from="feeling", table_from="feeling_h5_rt", table_to="show_feeling_h5_rt", db_to="produce")

        # worker.move_table(db_from = "jh_10a0e81221095bdba91f7688941948a6", table_from="biqu_all_all_market_hy_7", table_to="show_jiayu_ios_ios_market_hy_7", db_to="produce")
        # worker.move_table(db_from = "caiyu", table_from="caiyu_ios_free_ios_market_hy_7", table_to="show_jkp_ios_ios_market_hy_7", db_to="produce")
        # worker.clear_db()

        # worker.create_data_appkeyA_appkeyB(start_day='2017-05-02', end_day='2017-05-18', seed_start_day='2017-01-01', seed_end_day='2017-04-20', db_from="jh_10a0e81221095bdba91f7688941948a6", appkey_from="biqu_all", appkey_to="jiayu_ios", plat_from="all", plat_to = 'ios', db_to="produce")
        # worker.update_overall("produce", "show_jiayu_ios_ios_overall", min_tm = None, max_tm = None, colname_day="tm")
        # worker.create_data_appkeyA_appkeyB(start_day='2017-05-02', end_day='2017-05-18', seed_start_day='2017-01-01', seed_end_day='2017-05-16', db_from="caiyu", appkey_from="caiyu_ios_free", appkey_to="jkp_ios", plat_from="ios", plat_to='ios', db_to="produce")
        # worker.update_overall("produce", "show_jkp_ios_ios_overall", min_tm=None, max_tm=None, colname_day="tm")

        # worker.create_data_appkeyA_appkeyB(start_day='2016-11-09', end_day='2017-05-19', seed_start_day = '2017-01-01', seed_end_day = '2017-05-17', db_from = "jh_10a0e81221095bdba91f7688941948a6", appkey_from = "biqu_all", appkey_to = "jkp_ios", plat_from = "all", plat_to = 'ios', db_to = "produce")
        # worker.move_table(db_from="jh_10a0e81221095bdba91f7688941948a6", table_from="biqu_all_all_overall_week", table_to="show_jkp_ios_ios_overall_week", db_to="produce")
        # worker.update_overall("produce", "show_jkp_ios_ios_overall", min_tm=None, max_tm=None, colname_day="tm")
        # worker.move_table(db_from = "jh_10a0e81221095bdba91f7688941948a6", table_from="biqu_all_all_market_hy_7", table_to="show_jkp_ios_ios_market_hy_7", db_to="produce")

        # # 伪造 够轻松 1000~，2016-06-29
        # worker.create_data_appkeyA_appkeyB(start_day = '2017-05-02', end_day = '2017-05-21', seed_start_day = '2017-01-02', seed_end_day = '2017-03-03', db_from = "caiyu", appkey_from = "caiyu_ad", appkey_to = "gouqs", plat_from = "android", plat_to = 'ios', db_to = "produce")
        # worker.move_table(db_from="caiyu", table_from="caiyu_ad_android_overall_week", table_to="show_gouqs_ios_overall_week", db_to="produce")
        # worker.move_table(db_from="caiyu", table_from="caiyu_ios_free_ios_ua", table_to="show_gouqs_ios_ua", db_to="produce")
        # worker.move_table(db_from="caiyu", table_from="caiyu_ios_free_ios_os", table_to="show_gouqs_ios_os", db_to="produce")
        # worker.update_overall("produce", "show_gouqs_ios_overall", min_tm=None, max_tm=None, colname_day="tm")
        # worker.move_table(db_from="caiyu", table_from="caiyu_ad_android_market_hy_7", table_to="show_gouqs_ios_market_hy_7", db_to="produce")
        #
        # 伪造 美滴滴 300~500，2016-09-28
        # worker.create_data_appkeyA_appkeyB(start_day = '2017-05-20', end_day = '2017-05-21', seed_start_day = '2016-08-25', seed_end_day = '2016-11-10', db_from = "guaeng", appkey_from = "hbtv", appkey_to = "meidd", plat_from = "android", plat_to = 'android', db_to = "produce")
        # worker.move_table(db_from="guaeng", table_from="hbtv_android_overall_week", table_to="show_meidd_ios_overall_week", db_to="produce")
        # worker.move_table(db_from="guaeng", table_from="huiyue_ios_ios_ua", table_to="show_meidd_ios_ua", db_to="produce")
        # worker.move_table(db_from="guaeng", table_from="huiyue_ios_ios_os", table_to="show_meidd_ios_os", db_to="produce")
        # worker.update_overall("produce", "show_meidd_ios_overall", min_tm=None, max_tm=None, colname_day="tm")
        # worker.move_table(db_from="guaeng", table_from="hbtv_android_market_hy_7", table_to="show_meidd_ios_market_hy_7", db_to="produce")
        # worker.move_table(db_from="guaeng", table_from="hbtv_android_event", table_to="show_meidd_ios_event", db_to="produce")
        # worker.move_table(db_from="guaeng", table_from="hbtv_android_page", table_to="show_meidd_ios_page", db_to="produce")

        # 伪造 walkup 5000~，2016-05-25
        # worker.create_data_appkeyA_appkeyB(start_day = '2017-05-21', end_day = '2017-05-21', seed_start_day = '2017-01-04', seed_end_day = '2017-03-04', db_from = "caiyu", appkey_from = "caiyu_ad", appkey_to = "walkup", plat_from = "android", plat_to = 'ios', db_to = "produce")
        # worker.move_table(db_from="caiyu", table_from="caiyu_ad_android_overall_week", table_to="show_walkup_ios_overall_week", db_to="produce")
        # worker.move_table(db_from="caiyu", table_from="caiyu_ios_free_ios_ua", table_to="show_walkup_ios_ua", db_to="produce")
        # worker.move_table(db_from="caiyu", table_from="caiyu_ios_free_ios_os", table_to="show_walkup_ios_os", db_to="produce")
        # worker.update_overall("produce", "show_walkup_ios_overall", min_tm=None, max_tm=None, colname_day="tm")
        # worker.move_table(db_from="caiyu", table_from="caiyu_ad_android_market_hy_7", table_to="show_walkup_ios_market_hy_7", db_to="produce")
        # worker.large_data_daily(num)

    if "daily" in sys.argv:
        worker = GetData()
        num = 1
        start_day = (datetime.datetime.now() - datetime.timedelta(days=num)).strftime("%Y-%m-%d")
        end_day = start_day
        for db_name in worker.get_dbs():
            if db_name in ['jiayu', 'jkp', 'meidd', 'walkup', 'gouqs']:
                continue
            worker.db_rsync_byday(num, db_name)

        # 伪造 够轻松 1000~，2016-06-30
        worker.create_data_appkeyA_appkeyB(start_day = start_day, end_day = end_day, seed_start_day = '2017-01-02', seed_end_day = '2017-03-03', db_from = "caiyu", appkey_from = "caiyu_ad", appkey_to = "gouqs", plat_from = "android", plat_to = 'ios', db_to = "produce")
        worker.move_table(db_from="caiyu", table_from="caiyu_ad_android_overall_week", table_to="show_gouqs_ios_overall_week", db_to="produce")
        worker.move_table(db_from="caiyu", table_from="caiyu_ios_free_ios_ua", table_to="show_gouqs_ios_ua", db_to="produce")
        worker.move_table(db_from="caiyu", table_from="caiyu_ios_free_ios_os", table_to="show_gouqs_ios_os", db_to="produce")
        worker.update_overall("produce", "show_gouqs_ios_overall", min_tm=None, max_tm=None, colname_day="tm")
        worker.move_table(db_from="caiyu", table_from="caiyu_ad_android_market_hy_7", table_to="show_gouqs_ios_market_hy_7", db_to="produce")
        worker.move_table(db_from="caiyu", table_from="caiyu_ad_android_market_remain", table_to="show_gouqs_ios_market_remain", db_to="produce")

        # 伪造 walkup 5000~，2016-05-25
        worker.create_data_appkeyA_appkeyB(start_day = start_day, end_day = end_day, seed_start_day = '2017-01-04', seed_end_day = '2017-03-04', db_from = "caiyu", appkey_from = "caiyu_ad", appkey_to = "walkup", plat_from = "android", plat_to = 'ios', db_to = "produce")
        worker.move_table(db_from="caiyu", table_from="caiyu_ad_android_overall_week", table_to="show_walkup_ios_overall_week", db_to="produce")
        worker.move_table(db_from="caiyu", table_from="caiyu_ios_free_ios_ua", table_to="show_walkup_ios_ua", db_to="produce")
        worker.move_table(db_from="caiyu", table_from="caiyu_ios_free_ios_os", table_to="show_walkup_ios_os", db_to="produce")
        worker.update_overall("produce", "show_walkup_ios_overall", min_tm=None, max_tm=None, colname_day="tm")
        worker.move_table(db_from="caiyu", table_from="caiyu_ad_android_market_hy_7", table_to="show_walkup_ios_market_hy_7", db_to="produce")
        worker.move_table(db_from="caiyu", table_from="caiyu_ad_android_market_remain", table_to="show_walkup_ios_market_remain", db_to="produce")
        worker.large_data_daily(num)

        # 伪造 美滴滴 300~500，2016-09-28
        worker.create_data_appkeyA_appkeyB(start_day = start_day, end_day = end_day, seed_start_day = '2016-08-25', seed_end_day = '2016-11-10', db_from = "guaeng", appkey_from = "hbtv", appkey_to = "meidd", plat_from = "android", plat_to = 'android', db_to = "produce")
        worker.move_table(db_from="guaeng", table_from="hbtv_android_overall_week", table_to="show_meidd_ios_overall_week", db_to="produce")
        worker.move_table(db_from="guaeng", table_from="huiyue_ios_ios_ua", table_to="show_meidd_ios_ua", db_to="produce")
        worker.move_table(db_from="guaeng", table_from="huiyue_ios_ios_os", table_to="show_meidd_ios_os", db_to="produce")
        worker.update_overall("produce", "show_meidd_ios_overall", min_tm=None, max_tm=None, colname_day="tm")
        worker.move_table(db_from="guaeng", table_from="hbtv_android_market_hy_7", table_to="show_meidd_ios_market_hy_7", db_to="produce")
        worker.move_table(db_from="guaeng", table_from="hbtv_android_market_remain", table_to="show_meidd_ios_market_remain", db_to="produce")

        # 家语 伪造数据
        worker.create_data_appkeyA_appkeyB(start_day = start_day, end_day = end_day, seed_start_day = '2017-01-01', seed_end_day = end_day, db_from = "jh_10a0e81221095bdba91f7688941948a6", appkey_from = "biqu_all", appkey_to = "jiayu_ios", plat_from = "all", plat_to = 'ios', db_to = "produce")
        worker.move_table(db_from="jh_10a0e81221095bdba91f7688941948a6", table_from="biqu_all_all_overall_week", table_to="show_jiayu_ios_ios_overall_week", db_to="produce")
        worker.move_table(db_from="jh_10a0e81221095bdba91f7688941948a6", table_from="biqu_all_all_market_remain", table_to="show_jiayu_ios_ios_market_remain", db_to="produce")
        worker.update_overall("produce", "show_jiayu_ios_ios_overall", min_tm=None, max_tm=None, colname_day="tm")
        worker.move_table(db_from="jh_10a0e81221095bdba91f7688941948a6", table_from="biqu_all_all_market_hy_7", table_to="show_jiayu_ios_ios_market_hy_7", db_to="produce")

        # 酒靠谱 伪造数据
        worker.create_data_appkeyA_appkeyB(start_day = start_day, end_day = end_day, seed_start_day = '2017-01-01', seed_end_day = end_day, db_from = "jh_10a0e81221095bdba91f7688941948a6", appkey_from = "biqu_all", appkey_to = "jkp_ios", plat_from = "all", plat_to = 'ios', db_to = "produce")
        worker.move_table(db_from="jh_10a0e81221095bdba91f7688941948a6", table_from="biqu_all_all_overall_week", table_to="show_jkp_ios_ios_overall_week", db_to="produce")
        worker.move_table(db_from="jh_10a0e81221095bdba91f7688941948a6", table_from="biqu_all_all_market_remain", table_to="show_jkp_ios_ios_market_remain", db_to="produce")
        worker.update_overall("produce", "show_jkp_ios_ios_overall", min_tm=None, max_tm=None, colname_day="tm")
        worker.move_table(db_from = "jh_10a0e81221095bdba91f7688941948a6", table_from="biqu_all_all_market_hy_7", table_to="show_jkp_ios_ios_market_hy_7", db_to="produce")
        worker.move_table(db_from="guaeng", table_from="hbtv_android_event", table_to="show_jkp_ios_event", db_to="produce")
        worker.move_table(db_from="guaeng", table_from="hbtv_android_page", table_to="show_jkp_ios_page", db_to="produce")
        worker.move_table(db_from="guaeng", table_from="hbtv_android_loc", table_to="show_jkp_ios_loc", db_to="produce")


    if "hour" in sys.argv:
        worker = GetData()
        for db_name in worker.get_dbs():
            if db_name in ['jiayu', 'jkp', 'meidd', 'walkup', 'gouqs']:
                continue
            worker.db_rsync_byhour(db_name)


        # 伪造 够轻松 1000~，2016-06-30
        worker.move_table(db_from="caiyu", table_from="caiyu_ios_free_ios_overall_rt_byhour", table_to="show_gouqs_ios_overall_rt_byhour", db_to="produce")
        worker.move_table(db_from="caiyu", table_from="caiyu_ios_free_ios_event_rt_byhour", table_to="show_gouqs_ios_event_rt_byhour", db_to="produce")
        # 伪造 walkup 5000~，2016-05-25
        worker.move_table(db_from="caiyu", table_from="caiyu_ios_free_ios_overall_rt_byhour", table_to="show_walkup_ios_overall_rt_byhour", db_to="produce")
        worker.move_table(db_from="caiyu", table_from="caiyu_ios_free_ios_event_rt_byhour", table_to="show_walkup_ios_event_rt_byhour", db_to="produce")
        # 伪造 美滴滴 300~500，2016-09-28
        worker.move_table(db_from="guaeng", table_from="huiyue_ios_ios_overall_rt_byhour", table_to="show_meidd_ios_overall_rt_byhour", db_to="produce")
        worker.move_table(db_from="guaeng", table_from="huiyue_ios_ios_event_rt_byhour", table_to="show_meidd_ios_event_rt_byhour", db_to="produce")


        # jiayu 伪造数据
        worker.move_table(db_from="jh_10a0e81221095bdba91f7688941948a6", table_from="biqu_all_all_event_rt_byhour", table_to="show_jiayu_ios_ios_event_rt_byhour", db_to="produce")
        worker.move_table(db_from="jh_10a0e81221095bdba91f7688941948a6", table_from="biqu_all_all_overall_rt_byhour", table_to="show_jiayu_ios_ios_overall_rt_byhour", db_to="produce")

        # jkp 伪造数据
        worker.move_table(db_from="caiyu", table_from="caiyu_ad_android_event_rt_byhour", table_to="show_jkp_ios_ios_event_rt_byhour", db_to="produce")
        worker.move_table(db_from="caiyu", table_from="caiyu_ad_android_overall_rt_byhour", table_to="show_jkp_ios_ios_overall_rt_byhour", db_to="produce")

        # feeling 订制化
        worker.move_table(db_from="feeling", table_from="feeling_ios_overall_rt_byhour", table_to="show_feeling_ios_overall_rt_byhour", db_to="produce")
        worker.move_table(db_from="feeling", table_from="feeling_ios_event_rt_byhour", table_to="show_feeling_ios_event_rt_byhour", db_to="produce")
        worker.move_table(db_from="feeling", table_from="feeling_h5_rt", table_to="show_feeling_h5_rt", db_to="produce")

    # tester.data_regression()

    # tester.create_data("2016-12-06", "2017-03-16", "2017-03-17", "2017-05-15", db_from="ncf")

    # tester.clear_db()
    #
    # for db_name in tester.get_dbs():
    #     if db_name not in ["caiyu"]:
    #         continue
    #     tester.db_rsync_all(db_name)

    # for db_name in tester.get_dbs():
    #     if db_name in ["caiyu"]:
    #         continue
    #     # tester.db_rsync_byhour(db_name)
    #     tester.db_rsync_byday(1, db_name)



