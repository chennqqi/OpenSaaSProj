# coding=utf-8
import MySQLdb
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

global mysql_host, mysql_port, mysql_user, mysql_passwd
mysql_host, mysql_port, mysql_user, mysql_passwd = "injhkj01.mysql.rds.aliyuncs.com", 3306, "jhkj", "jhkj_jhkj"


class MysqlClient(object):

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

    # def getAppkey(self):
    #     result = []
    #     sql = "select a.appkey, b.cdkey, a.plat from (select * from d_app where enable = 1) a left join (select * from d_account where enable = 1) b on a.own = b.name_uid"
    #     self.cur.execute(sql)
    #     for item in self.cur.fetchall():
    #         appkey, dbname, plat = item[0], item[1], item[2]
    #         result.append((dbname, appkey, plat))
    #     return result

    def getAppkey(self):
        result = []
        sql = "select a.appkey, b.cdkey, a.plat from (select * from saas_meta.d_app where enable = 1 and (plat = 'android' or plat = 'ios' or plat = 'feeling' or plat = 'all')) a left join (select * from saas_meta.d_account where enable = 1) b on a.own = b.name_uid"
        self.cur.execute(sql)
        for item in self.cur.fetchall():
            appkey, dbname, plat = item[0], item[1], item[2]
            # appkey = appkey.lower()
            result.append((dbname, appkey, plat))
        return result

    def getAppkey_app(self, plat=["android", "ios", "feeling", "all"]):
        return self.getAppkey_kwargs(plat=plat)

    def getAppkey_H5(self):
        return self.getAppkey_kwargs(plat = ["h5"])

    def getAppkey_kwargs(self, **kwargs):
        result = []
        columns_name = ["appkey", "plat", "mongo_id", "cdkey"]
        sql = "SELECT appkey, plat, mongo_id, cdkey FROM saas_server.d_appkey WHERE enable = 1"
        self.cur.execute(sql)
        for item in self.cur.fetchall():
            column_filter = [True] * len(item)
            for index, column in enumerate(columns_name):
                if kwargs.get(column, None) != None:
                    if isinstance(kwargs[column], list):
                        if not any([item[index] == kwargs[column][i] for i in range(0, len(kwargs[column]))]):
                            column_filter[index] = False
                            break
                    elif item[index] != kwargs[column]:
                        column_filter[index] = False
                        break
            if all(column_filter):
                result.append(dict(zip(columns_name, item)))
        # 除去重复项
        result_project = []
        filter_container = set([])
        filter_keys = kwargs.get("filter_keys", ["appkey"])
        for item in result:
            elem = tuple([item[key] for key in filter_keys])
            if elem in filter_container:
                continue
            item.pop("mongo_id")
            result_project.append(item)
            filter_container.add(elem)
        return result_project

    def closeMysql(self):
        self.cur.close()
        self.con.close()


if __name__ == "__main__":
    # if "store_appkey" in sys.argv:
    tester = MysqlClient("saas_server")
    print len(tester.getAppkey()), tester.getAppkey()
    print len(tester.getAppkey_app()), tester.getAppkey_app()
