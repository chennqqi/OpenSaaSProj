# --coding=utf8--
import os
import os.path as path
from SaaSCommon.JHOpen import JHOpen
import json
from SaaSMode.EventDetailH5 import H5EventDetail
import datetime
from DBClient.PostgreSqlClient import PostgreSqlClient


if __name__ == "__main__":
    dataPath = os.sep.join([path.dirname(path.abspath(__file__)), "data"])
    inputStream = JHOpen.readLines(dataPath)
    data = []
    for line in inputStream:
        item = dict(json.loads(line))
        temp = H5EventDetail()
        type = item.get("type")
        if type == "page":
            temp.support = item.get("support")
            temp.usermap = item.get("usermap")
            temp.ref = item.get("ref")
        elif type == "dur":
            temp.status = item.get("status")
            temp.value = item.get("value")
        elif type == "ac":
            temp.event = item.get("event")
        temp.appkey = item.get("appkey")
        temp.type = item.get("type")
        temp.uri = item.get("uri")
        temp.uid = item.get("uid")
        temp.opatime = datetime.datetime.fromtimestamp(item.get("ts")/1000)
        temp.vr = item.get("vr")
        temp.device = item.get("device")
        temp.system = item.get("system")
        temp.browser = item.get("browser")
        temp.screen = item.get("screen")
        temp.ua = item.get("ua")
        temp.ts = datetime.date.today() - datetime.timedelta(1)
        data.append(temp.build())
    print data

    client = PostgreSqlClient("jh_10a0e81221095bdba91f7688941948a6")
    try:
        client.cur.execute("DELETE FROM biqu_h5_event_detail where ts = '%s'" % (datetime.date.today() - datetime.timedelta(1)))
        sql = "INSERT INTO biqu_h5_event_detail (id, appkey, type, status, value, uri, uid, opatime, vr, device, system, browser, screen, ua, ts, support, usermap, ref, event)" \
              " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        client.cur.executemany(sql, data)
        client.con.commit()
    except Exception, e:
        print e
        client.con.rollback()
    finally:
        client.close()




