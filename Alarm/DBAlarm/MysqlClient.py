#coding=utf-8
from __init__ import configPath
import MySQLdb
import ConfigParser
import sys
reload(sys)
sys.setdefaultencoding("utf-8")



class MysqlClient(object):
    cf = ConfigParser.ConfigParser()
    cf.read(configPath)
    mysql_host = cf.get("mysqldb", "mysql_host")
    mysql_port = cf.getint("mysqldb", "mysql_port")
    mysql_user = cf.get("mysqldb", "mysql_user")
    mysql_passwd = cf.get("mysqldb", "mysql_passwd")

    def __init__(self, db, host=mysql_host, port=mysql_port, user=mysql_user, passwd=mysql_passwd):
        self.db = db
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.con, self.cur = self._connectMysql

    @property
    def _connectMysql(self):
        conn = MySQLdb.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                passwd=self.passwd,
                db=self.db,
                charset='utf8'
                )
        cur = conn.cursor()
        return conn, cur

    def select(self, cmd):
        assert cmd.startswith("select")
        self.cur.execute(cmd)
        for item in self.cur.fetchall():
            yield item

    def getAppkey(self):
        result = []
        sql = "select a.appkey, b.cdkey, a.plat from (select * from saas_meta.d_app where enable = 1) a left join (select * from saas_meta.d_account where enable = 1) b on a.own = b.name_uid"
        self.cur.execute(sql)
        for item in self.cur.fetchall():
            appkey, dbname, plat = item[0], item[1], item[2]
            result.append((dbname, appkey, plat))
        return result

    def closeMysql(self):
        self.cur.close()
        self.con.close()


if __name__ == "__main__":
    tester = MysqlClient("saas_meta")
    print(tester.getAppkey())
    tester.closeMysql()