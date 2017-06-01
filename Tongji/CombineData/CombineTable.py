# -*- coding: utf-8 -*-
from CombineTableBasic import CombineTableBasic
from DBClient.MysqlClient import MysqlClient
from collections import OrderedDict
import time
import datetime

def getWeekFirstDay(last_week, startDay = time.strftime("%Y-%m-%d", time.localtime(time.time())), dateformat = "%Y%m%d"):
    from datetime import datetime
    from datetime import timedelta
    last_week_end_day = (datetime.fromtimestamp(time.mktime(time.strptime(startDay.replace("-", ""), "%Y%m%d"))) -
                         timedelta(weeks=last_week)).isoweekday()
    result = {}
    day = (datetime.fromtimestamp(time.mktime(time.strptime(startDay.replace("-", ""), "%Y%m%d"))) -
    timedelta(days=last_week_end_day+(last_week-1)*7+7-1)).strftime(dateformat)
    result.setdefault(day, last_week_end_day+(last_week-1)*7+7-1)
    return result

class CombineTable(CombineTableBasic):

    def __init__(self):
        self.client = MysqlClient("information_schema")
        self.con, self.cur = self.client.connection

    def createtablelike(self, dbname, tablename, liketablename):
        sql_format = "CREATE TABLE IF NOT EXISTS %(dbname)s.%(tablename)s LIKE %(dbname)s.%(liketablename)s"
        sql = sql_format % {"dbname": dbname, "tablename": tablename, "liketablename": liketablename}
        self.cur.execute(sql)
        self.con.commit()

    def combinetable_fromappkey(self, num, dbname, appkey_plat_pairs, tablename, combineappkey):
        tablename1 = "%(appkey)s_%(plat)s_%(tablename)s" % {
            "appkey": appkey_plat_pairs[0][0],
            "plat": appkey_plat_pairs[0][1],
            "tablename": tablename
        }
        tablename2 = "%(appkey)s_%(plat)s_%(tablename)s" % {
            "appkey": appkey_plat_pairs[1][0],
            "plat": appkey_plat_pairs[1][1],
            "tablename": tablename
        }
        combinetablename = "%(appkey)s_%(plat)s_%(tablename)s" % {
            "appkey": combineappkey,
            "plat": "all",
            "tablename": tablename
        }
        if 'week' in tablename1:
            yyyymmdd = time.strftime("%Y%m%d", time.localtime(time.time() - 86400 * num))
            week_day = datetime.datetime.strptime(yyyymmdd, "%Y%m%d").weekday()
            if week_day == 6:
                tm = time.strftime("%Y-%m-%d", time.localtime(time.time()))
                num = getWeekFirstDay(1, tm).values()[0] + num - 1
        self.createtablelike(dbname, combinetablename, tablename1)
        if "remain" in tablename1:
            self.combinetable_all("all", dbname, tablename1, tablename2, combinetablename)
        else:
            self.combinetable(num, dbname, tablename1, tablename2, combinetablename)
        self.con.commit()

    def cleartable(self, num, dbname, tablename, tm_columnname):
        if num == "all":
            delete_sql_format = "truncate table %(dbname)s.%(tablename)s"
            delete_sql = delete_sql_format % {
                "dbname": dbname,
                "tablename": tablename
            }
        else:
            tm = time.strftime("%Y-%m-%d", time.localtime(time.time() - 86400 * num))
            delete_sql_format = "DELETE FROM %(dbname)s.%(tablename)s WHERE %(tm_columnname)s = '%(tm)s'"
            delete_sql = delete_sql_format % {
                "dbname": dbname,
                "tablename": tablename,
                "tm_columnname": tm_columnname,
                "tm": tm
            }
        self.cur.execute(delete_sql)
        self.con.commit()

    def combinetable_all(self, num, dbname, tablename1, tablename2, combinetablename):
        combine_columns, tm_columnname = self.combinecolumns(dbname, tablename1, tablename2)
        self.cleartable("all", dbname, combinetablename, tm_columnname)
        result = {}
        select_sql_format = "SELECT %(combinekeys)s, -666, %(combinevalues)s FROM %(dbname)s.%(tablename)s"
        insert_sql_format = "INSERT INTO %(dbname)s.%(tablename)s (%(combinekeys)s, %(combinevalues)s) VALUES (%(colvalues)s)"
        tables = [tablename1, tablename2]
        combinekeys = self.get_combinekey(combine_columns)
        combinevalues = self.get_combinevalue(combine_columns)
        for sql in [select_sql_format % {
            "combinekeys": ", ".join(["tm"] + combinekeys),
            "combinevalues": ", ".join(combinevalues),
            "dbname": dbname,
            "tablename": tablename,
            "tm_columnname": tm_columnname,
        }
                    for tablename in tables]:
            for item in self.client.select(sql):
                item = list(item)
                key = tuple([key for key in item[:item.index(-666)]])
                defaultvalue = [None for i in range(0, len(item[item.index(-666) + 1:]))]
                value = [i for i in item[item.index(-666) + 1:]]
                # 初始化
                result.setdefault(key, defaultvalue)
                result[key] = [((i if i else 0.0) + (j if j else 0.0)) if not (i is None and j is None) else None
                               for i, j in zip(result[key], value)]

        for key_items in result:
            try:
                insert_sql = insert_sql_format % {
                    "dbname": dbname,
                    "tablename": combinetablename,
                    "combinekeys": ", ".join([tm_columnname] + combinekeys),
                    "combinevalues": ", ".join(combinevalues),
                    "colvalues": ", ".join([
                        "'" + "', '".join(map(str, key_items)) + "'",
                        ", ".join(map(str, ['null' if item is None else item for item in result[key_items]])),
                    ]),
                }
                self.cur.execute(insert_sql)
            except:
                import traceback
                print(traceback.print_exc())
                print("insert faild", key_items, result[key_items])

    def combinetable(self, num, dbname, tablename1, tablename2, combinetablename):
        combine_columns, tm_columnname = self.combinecolumns(dbname, tablename1, tablename2)
        tm = time.strftime("%Y-%m-%d", time.localtime(time.time() - 86400*num))
        self.cleartable(num, dbname, combinetablename, tm_columnname)
        result = {}
        select_sql_format = "SELECT %(combinekeys)s, -666, %(combinevalues)s FROM %(dbname)s.%(tablename)s WHERE %(tm_columnname)s = '%(tm)s'"
        insert_sql_format = "INSERT INTO %(dbname)s.%(tablename)s (%(combinekeys)s, %(combinevalues)s) VALUES (%(colvalues)s)"
        tables = [tablename1, tablename2]
        combinekeys = self.get_combinekey(combine_columns)
        combinevalues = self.get_combinevalue(combine_columns)
        for sql in [select_sql_format % {
            "combinekeys": ", ".join(combinekeys),
            "combinevalues": ", ".join(combinevalues),
            "dbname": dbname,
            "tablename": tablename,
            "tm_columnname": tm_columnname,
            "tm": tm
        }
                    for tablename in tables]:
            for item in self.client.select(sql):
                item = list(item)
                key = tuple([key for key in item[:item.index(-666)]])
                defaultvalue = [None for i in range(0, len(item[item.index(-666)+1:]))]
                value = [i for i in item[item.index(-666)+1:]]
                # 初始化
                result.setdefault(key, defaultvalue)
                result[key] = [((i if i else 0.0) + (j if j else 0.0)) if not (i is None and j is None) else None
                               for i, j in zip(result[key], value)]

        for key_items in result:
            try:
                insert_sql = insert_sql_format % {
                    "dbname": dbname,
                    "tablename": combinetablename,
                    "combinekeys": ", ".join([tm_columnname] + combinekeys),
                    "combinevalues": ", ".join(combinevalues),
                    "colvalues": ", ".join([
                                                "'" + tm + "'",
                                                "'" + "', '".join(map(str, key_items)) + "'",
                                                ", ".join(map(str, ['null' if item is None else item for item in result[key_items]])),
                                           ]),
                }
                self.cur.execute(insert_sql)
            except:
                import traceback
                print(traceback.print_exc())
                print("insert faild", key_items, result[key_items])


    def get_combinekey(self, combine_columns):
        result = set()
        for col_name, data_type in combine_columns.items():
            if data_type.upper() in ("VARCHAR", "CHAR", "TEXT"):
                result.add(col_name)
        result = list(result)
        result.sort()
        return result

    def get_combinevalue(self, combine_columns):
        result = set()
        for col_name, data_type in combine_columns.items():
            if data_type.upper() in ("TINYINT", "SMALLINT", "MEDIUMINT", "INT", "INTEGER", "BIGINT", "FLOAT",
                                     "DOUBLE", "DECIMAL"):
                result.add(col_name)
        result = list(result)
        result.sort()
        return result

    def combinecolumns(self, dbname, tablename1, tablename2):
        sql = "SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.COLUMNS \
WHERE table_name = '%(tablename)s' and table_schema = '%(dbname)s' and extra != 'auto_increment'"
        tm_columnname = None
        columns_list = set()
        columns = OrderedDict()
        for tablename in [tablename1, tablename2]:
            for col_name, data_type in self.client.select(sql % {"dbname": dbname, "tablename": tablename}):
                if data_type in ["date", "datetime"]:
                    tm_columnname = col_name
                    continue
                columns_list.add((col_name, data_type))
        columns_list = list(columns_list)
        columns_list.sort()
        for col_name, data_type in columns_list:
            columns.setdefault(col_name, data_type)
        return columns, tm_columnname

if __name__ == "__main__":
    num = 2
    tm = time.strftime("%Y-%m-%d", time.localtime(time.time() - 86400 * (num - 1)))
    print(getWeekFirstDay(1, tm))
    print(getWeekFirstDay(0, tm).values()[0] + 1)
    # tester = CombineTable()
    # tester.combinetable_fromappkey(1, "jh_10a0e81221095bdba91f7688941948a6", (("biqu", "ios"), ("biqu_android", "android")), "overall", "biqu")
    # print tester.combinecolumns("caiyu", "caiyu_ad_android_event_rt_byhour", "caiyu_ad_android_event_rt_byhour")
    # tester.combinetable(1, "jh_10a0e81221095bdba91f7688941948a6", "biqu_android_android_overall", "biqu_ios_overall", None)
    # tester.createtable("jh_10a0e81221095bdba91f7688941948a6", "create_table_py", "biqu_android_android_overall")



