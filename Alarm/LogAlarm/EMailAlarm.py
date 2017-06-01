# -*- coding: utf-8 -*-
import __init__
import os
import datetime
import json

from EMail import EMail


class EMailAlarm(object):

    @staticmethod
    def check_path(path):

        if os.path.exists(path):
            return True
        else:
            return False

    @staticmethod
    def send(comtent):
        email = EMail()
        email.send(u"汇总日志缺失！", comtent)


def get_appkeys():
    from DBAlarm.MysqlClient import MysqlClient
    client = MysqlClient("saas_server")
    result = {}
    for item in client.select("select appkey, plat from saas_server.d_appkey where enable = 1"):
        appkey = item[0]
        plat = item[1]
        result.setdefault(appkey, []).append(plat)
    return result

path_format = "/data1/logs/transformsaaslogs/%(appkey)s/%(yyyymmdd)s/%(hhmm)s.log.gz"


def check(day = 0, minute = 10, last = 30, appkeys = None):
    if appkeys is None:
        appkeys = get_appkeys()
    result = {}
    for appkey in appkeys:
        for i in range(0, last+1):
            hhmm = (datetime.datetime.now() - datetime.timedelta(days=day) - datetime.timedelta(seconds=60 * (minute+i))).strftime("%H%M")
            yyyymmdd = (datetime.datetime.now() - datetime.timedelta(days=day) - datetime.timedelta(seconds=60 * (minute+i))).strftime("%Y%m%d")
            path = path_format % {"appkey": appkey, "yyyymmdd": yyyymmdd, "hhmm": hhmm}
            result.setdefault(appkey, []).append(EMailAlarm.check_path(path))
    send_message = []
    for appkey in result:
        if all(map(lambda item: item == False, result[appkey])):
            send_message.append(appkey)
    EMailAlarm().send(json.dumps(send_message))


if __name__ == "__main__":
    check()

