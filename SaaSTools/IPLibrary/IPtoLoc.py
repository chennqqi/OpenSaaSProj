# -*- coding: utf-8 -*-
import urllib
import json
from abc import abstractmethod
from abc import ABCMeta

class IPtoLocBasic:

    __metaclass__ = ABCMeta

    @abstractmethod
    def loc(self, ip):
        pass

class IPtoLoc(IPtoLocBasic):

    def __init__(self):
        self.uriformat = 'http://int.dpool.sina.com.cn/iplookup/iplookup.php?format=json&ip=%(ip)s'

    def setURIFormat(self, uri):
        self.uriformat = uri

    def IPtoLong(self, strIP):
        ip = strIP.split('.')
        return (long(ip[0]) << 24) + (long(ip[1]) << 16) + (long(ip[2]) << 8) + long(ip[3])

    def loc(self, ip):
        if not ip:
            return (None, 'unknown', 'unknown', 'unknown')
        ip = ip.replace("_", ".").strip('''"''')
        params = {"ip": ip}
        uri = self.uriformat % params
        try:
            text = urllib.urlopen(uri % {"ip": ip}).read()
            json_obj = json.loads(text)
            country = json_obj["country"]
            pro = json_obj["province"]
            city = json_obj["city"] if json_obj["city"] else pro
            iplong = self.IPtoLong(ip)
            result = (iplong, country if country else "unknown", pro if pro else "unknown", city if city else "unknown")
        except:
            result = (None, 'unknown', 'unknown', 'unknown')
        return result

if __name__ == "__main__":
    tester = IPtoLoc()
    ip = "8.8.8.8"
    print(tester.loc(ip)[0])
    print(tester.loc(ip)[1])
    print(tester.loc(ip)[2])
    print(tester.loc(ip)[3])