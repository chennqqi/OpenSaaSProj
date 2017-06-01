# coding: utf-8
import ConfigParser
from __init__ import configPath
from MysqlClient import MysqlClient
import time
import datetime
import urllib
from templete import templetes
import json
import sys


class MysqlAlarm(object):

    def __init__(self):
        self.cf = ConfigParser.ConfigParser()
        self.cf.read(configPath)
        self.sendmsg = self.cf.get("sendmsg", "msgaddress_high")

    def ischeck(self, client, dbname, appkey_plat):
        table = "_".join([appkey_plat, "overall"])
        tm = time.strftime("%Y-%m-%d", time.localtime(time.time()-86400*8))
        sql = "select count(1) from %(dbname)s.%(table)s where tm >= '%(tm)s'" % {"table": table, "dbname": dbname, "tm": tm}
        for item in client.select(sql):
            return item[0] != 0

    def get_alarm_user_phone(self, client):
        sql = "select name, phone, email, dbname from saas_meta.d_monitor_user where enable = 1"
        alarm_users = {}
        for item in client.select(sql):
            name, phone, email, dbname = item
            for _dbname in dbname.split("#"):
                for _iphone in phone.split("#"):
                    _iphone = _iphone.strip()
                    alarm_users.setdefault(_dbname, set()).add(_iphone)
        return alarm_users

    def saas_table_check(self, monitorfrq):
        meta_client = MysqlClient("saas_meta")
        saas_data = {}
        # {dbname: appkey_plat}
        [saas_data.setdefault(item[0], []).append("_".join([item[1].lower(), item[2].lower()])) for item in meta_client.getAppkey()] # appkey, dbname, plat
        sql = "select dbname, tablename, datefield, monitorfield, monitortype, monitorfrq, datedelay from saas_meta.d_monitor where enable = 1 and dbname = 'saas' and monitorfrq = '%s'" % monitorfrq
        check_tables = []
        for item in meta_client.select(sql):
            dbname, tablename, datefield, monitorfield, monitortype, monitorfrq, datedelay = item
            check_tables.append((tablename, datefield, monitorfield, monitortype, monitorfrq, datedelay))
        alarm_users = self.get_alarm_user_phone(meta_client)
        for dbname in saas_data:

            # table_head = saas_data[dbname]
            for table_head in saas_data[dbname]:
                empty_table = []
                if not self.ischeck(meta_client, dbname, table_head):
                    continue
                for item in check_tables:
                    tabletail, datefield, monitorfield, monitortype, monitorfrq, datedelay = item
                    tablename = "_".join([table_head, tabletail.strip("_")])
                    if monitorfrq == "day" and monitortype == "exists":
                        try:
                            notempty = self.check_byday(meta_client, dbname, tablename, datefield, monitorfield, delay_day=datedelay)
                            if not notempty:
                                empty_table.append((notempty, dbname, tabletail.strip("_")))
                        except:
                            import traceback
                            print(dbname, tablename, datefield, monitorfield, monitorfrq, datedelay)
                            print(traceback.print_exc())
                    elif monitorfrq == "hour" and monitortype == "exists":
                        try:
                            notempty = self.check_byhour(meta_client, dbname, tablename, datefield, monitorfield, delay_hour=datedelay)
                            if not notempty:
                                empty_table.append((notempty, dbname, tabletail.strip("_")))
                        except:
                            print(dbname, tablename, datefield, monitorfield, monitorfrq, datedelay)
                            import traceback
                            exc_type, exc_value, exc_traceback = sys.exc_info()
                            errinfo = traceback.format_exception(exc_type, exc_value, "")
                            print(errinfo)
                print "empty_table", empty_table
                if len(empty_table) > 0:
                    try:
                        phones = alarm_users.get("all", set()) | alarm_users.get("saas", set()) | alarm_users.get(dbname, set())
                        rec_num = ",".join(list(phones))
                        # print("saas empty table alarm(dbname: %s, appkey_plat: %s, table num: %d)"%(dbname, table_head, len(empty_table)), ",\\r\\n".join([u"@空表名称"]+[item[2] for item in empty_table]), rec_num)
                        self.alarm("saas empty table alarm(dbname: %s, appkey_plat: %s, table num: %d)"%(dbname, table_head, len(empty_table)), ",\\r\\n".join([u"@空表名称"]+[item[2] for item in empty_table]), rec_num)
                    except:
                        import traceback
                        exc_type, exc_value, exc_traceback = sys.exc_info()
                        errinfo = traceback.format_exception(exc_type, exc_value, "")
                        print(errinfo)
        meta_client.closeMysql()

    def customized_table_check(self, monitorfrq):
        meta_client = MysqlClient("saas_meta")
        dbtables = {}
        # {dbname: appkey_plat}
        sql = "select dbname, tablename, datefield, monitorfield, monitortype, monitorfrq, datedelay from saas_meta.d_monitor where enable = 1 and dbname != 'saas' and monitorfrq = '%s'" % monitorfrq
        for item in meta_client.select(sql):
            dbname, tablename, datefield, monitorfield, monitortype, monitorfrq, datedelay = item
            dbtables.setdefault(dbname, set()).add((tablename, datefield, monitorfield, monitortype, monitorfrq, datedelay))
            alarm_users = self.get_alarm_user_phone(meta_client)
        for dbname in dbtables:
            empty_table = []
            checktableinfo = dbtables[dbname]
            for item in checktableinfo:
                tablename, datefield, monitorfield, monitortype, monitorfrq, datedelay = item
                if monitorfrq == "day" and monitortype == "exists":
                    try:
                        notempty = self.check_byday(meta_client, dbname, tablename, datefield, monitorfield, delay_day=datedelay)
                        if not notempty:
                            empty_table.append((notempty, dbname, tablename))
                    except:
                        import traceback
                        print(traceback.print_exc())
                elif monitorfrq == "hour" and monitortype == "exists":
                    try:
                        notempty = self.check_byhour(meta_client, dbname, tablename, datefield, monitorfield, delay_hour=datedelay)
                        if not notempty:
                            empty_table.append((notempty, dbname, tablename))
                    except:
                        import traceback
                        print(traceback.print_exc())
            if len(empty_table) > 0:
                phones = alarm_users.get("all", set()) | alarm_users.get("custom", set()) | alarm_users.get(dbname, set())
                rec_num = ",".join(list(phones))
                # print "saas empty table alarm(dbname: %s, table num: %d)"%(dbname, len(empty_table)), ",\\r\\n".join([u"@空表名称"]+[item[2] for item in empty_table]), rec_num
                self.alarm("saas empty table alarm(dbname: %s, table num: %d)"%(dbname, len(empty_table)), ",\\r\\n".join([u"@空表名称"]+[item[2] for item in empty_table]), rec_num)
        meta_client.closeMysql()

    def alarm(self, title, content, rec_num, funcName="SMS_13620007", templete="default", recursion=0):
        recursion += 1
        ts = time.time()*1000
        data_dict = templetes[templete] % {"title": title, "content": content, "ts": ts, "funcName": "SMS_13620007", "rec_num": rec_num}
        send_address = self.sendmsg % urllib.quote(json.dumps(json.loads(data_dict)))
        print send_address
        response = urllib.urlopen(send_address)
        http_status = response.getcode()
        print("@%s"%time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts/1000.0)), "recursion: %d"%recursion, http_status, data_dict)
        if http_status == 200:
            return
        elif recursion <= 10: # 如果失败，重试10次
            response.close()
            time.sleep(1)
            self.alarm(title, content, rec_num, recursion=recursion)
        else:
            return

    def check_byhour(self, client, dbname, table, datefield, monitorfield, delay_hour = 1):
        yyyy_mm_dd_hhmiss = (datetime.datetime.strptime(time.strftime("%Y-%m-%d %H", time.localtime(time.time())), "%Y-%m-%d %H")
                             - datetime.timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
        sql = "select %(monitorfield)s from %(dbname)s.%(table)s where %(datefield)s >= '%(yyyy_mm_dd_hhmiss)s' and %(monitorfield)s is not null" \
              % {"monitorfield": monitorfield, "dbname": dbname, "table": table, "datefield": datefield, "yyyy_mm_dd_hhmiss": yyyy_mm_dd_hhmiss}
        print "hour", sql
        for item in client.select(sql):
            return True
        return False

    def check_byday(self, client, dbname, table, datefield, monitorfield, delay_day = 1):
        yyyy_mm_dd = time.strftime("%Y-%m-%d", time.localtime(time.time()-86400*delay_day))
        sql = "select %(monitorfield)s from %(dbname)s.%(table)s where %(datefield)s >= '%(yyyy_mm_dd)s' and %(monitorfield)s is not null" \
              % {"monitorfield": monitorfield, "dbname": dbname, "table": table, "datefield": datefield, "yyyy_mm_dd": yyyy_mm_dd}
        try:
            for item in client.select(sql):
                return True
            return False
        except:
            return True

    def check_byminute(self, delay_minute = 30):
        return True

if __name__ == "__main__":
    if "saas_daily" in sys.argv:
        alarm = MysqlAlarm()
        alarm.saas_table_check("day")

    if "custom_daily" in sys.argv:
        alarm = MysqlAlarm()
        alarm.customized_table_check("day")

    if "saas_hour" in sys.argv:
        alarm = MysqlAlarm()
        alarm.saas_table_check("hour")

    if "custom_hour" in sys.argv:
        alarm = MysqlAlarm()
        alarm.customized_table_check("hour")
