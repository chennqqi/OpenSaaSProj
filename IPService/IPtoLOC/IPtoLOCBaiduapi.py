# -*- coding: utf-8 -*-
import __init__
import json
import time
import logging
import requests
from IPtoLOC import IPtoLOC
from Loc import Loc
from os import sys
from WorkerLimit.WorkerLimit import WorkerLimit

logger = logging.getLogger(__file__)


class IPtoLOCBaiduapi(IPtoLOC):

    AK = 'paKQrVfPli2c5emzQ7nEtpEskqOitVDf'

    def __init__(self):
        self.url_format = "https://api.map.baidu.com/location/ip?ak=%(ak)s&coor=bd09ll&ip=%(ip)s"
        self.limiter = WorkerLimit(60, 180)

    def loc(self, ip, ak = AK):
        if self.limiter.remainCount() > 0:
            url = self.url_format % {"ip": ip, "ak": ak}
            r = requests.get(url, timeout=1)
            if r.status_code == 200:
                html = r.text
                data = json.loads(html)
                logging.info("Request %s, return code: %d" % (url, r.status_code))
            else:
                logging.warning("Request %s, return code: %d" % (url, r.status_code))
                return None
            self.limiter.inc()
            time.sleep(0.1)
        else:
            logger.info("self.limiter.remainCount() = %d, self.limiter.speed() = %.8f, Will sleep 10sec." % (self.limiter.remainCount(), self.limiter.speed()))
            time.sleep(10)
        logger.info("self.limiter.remainCount() = %d, self.limiter.speed() = %.8f, getURL Total: %d." % (self.limiter.remainCount(), self.limiter.speed(), self.limiter.getTotalInc()))
        try:
            data = self.format(data)
        except:
            logger.error(sys.exc_info() + "; " + html)
        return data

    def format(self, data):
        country = None
        province = data["content"]["address_detail"]["province"]
        city = data["content"]["address_detail"]["city"]
        district = None
        street = data["content"]["address_detail"]["street"]
        address = data["content"]["address"]
        lat = data["content"]["point"]["y"]
        lng = data["content"]["point"]["x"]
        # lat: 纬度，lng: 经度
        # 北京市区坐标为：北纬39.9”，东经116. 3”
        point = {"lng": lng, "lat": lat}
        loc = Loc()
        loc.setCountry(country)
        loc.setProvince(province)
        loc.setCity(city)
        loc.setDistrict(district)
        loc.setStreet(street)
        loc.setAddress(address)
        loc.setPoint(point)
        return loc.builder()

if __name__ == "__main__":
    tester = IPtoLOCBaiduapi()
    data = tester.loc("220.175.40.224")
    print type(data), json.dumps(data)
    print json.dumps(data, ensure_ascii=False).encode("utf-8")