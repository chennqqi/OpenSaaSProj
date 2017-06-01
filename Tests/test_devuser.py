# -*- coding: utf-8 -*-
import urllib
from DiskDict import DiskDict

url = '''http://123.59.147.152:21333/devusers/{"appkey":["all"],"iscache":true}'''
import urllib
import socket
import json


devUsers_diskdict = DiskDict(cache_name="devuser")
socket.setdefaulttimeout(2)
result = urllib.urlopen(url).read()
result = result.encode("utf-8")
print(type(result), result)
devUsers = json.loads(str(result))
devUsers = json.loads(json.dumps(devUsers, ensure_ascii=False))
for key in devUsers:
    print(type(key), key)
    key = key.encode('utf-8')
    print(type(key), key)
devUsers_diskdict.update(devUsers)



# print devUsers
print devUsers_diskdict