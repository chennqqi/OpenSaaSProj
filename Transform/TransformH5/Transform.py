# -*- coding: utf-8 -*-
import base64
import urllib
import re
import json
import time


class Transform(object):

    def __init__(self, timestamp = time.time()*1000):
        self.ip_pattern = re.compile(
            r'''"((?:(?:25[0-5]|2[0-4]\d|((1\d{2})|([1-9]?\d)))\.){3}(?:25[0-5]|2[0-4]\d|((1\d{2})|([1-9]?\d))))"''')
        self.timestamp = timestamp

    def transform(self, line):
        # if "params=" in line:
        #     data = line.split("params=")[1].split(" HTTP")[0]
        # elif "params%3d" in line:
        #     data = line.split("params=")[1].split(" HTTP")[0]
        # else:
        #     data = line.split("params=")[1].split(" HTTP")[0]

        params = line.split("params=")[1].split(" HTTP")[0]

        try:
            data = urllib.unquote(params)
            data = base64.b64decode(data)
            data = json.loads(data)
        except:
            params = line.split("params=")[1].split(" HTTP")[0]
            urlencode_guess = "unknown"
            if "%" not in params and "+" not in params:
                urlencode_guess = "no_urlencode"
            if "+" in params and "%" not in params:
                base64_guess = "yes_base64"
            if urlencode_guess != "no_urlencode":
                params = urllib.unquote(params)
            data = base64.b64decode(params)
            data = urllib.unquote(data)
            data = json.loads(data)

        # 如果为内网ip，做单独处理
        ip = line.split(",")[0].strip()
        try:
            if ip.startswith("127"):
                ip = self.ip_pattern.search(line).group(1)
        except:
            # import traceback
            # print(traceback.print_exc())
            ip = line.split(",")[0].strip()
        data["ip"] = ip
        # 从nigix日志中取出ua字段
        try:
            ua = line.split('"')[5]
            data.setdefault("ua", ua)
        except:
            import traceback
            print(traceback.print_exc())
        try:
            data["_jh_recv_ts"] = self.timestamp
        except:
            import traceback
            print traceback.print_exc()
        return data


if __name__ == "__main__":
    line = "http://cjsaas.jhddg.com/h5sta.gif?params=eyJhcHBrZXkiOiJ0ZXN0IiwidHlwZSI6InBhZ2UiLCJzdXBwb3J0IjoiMjEiLCJ1cmkiOiJodHRwOi8vMTkyLjE2OC4wLjIwOTo4MDgwL0ppbmdIb25nL2luZGV4Lmh0bWwiLCJyZWYiOiJodHRwOi8vMTkyLjE2OC4wLjIwOTo4MDgwL0ppbmdIb25nL2Fib3V0Lmh0bWwiLCJ1aWQiOiJ0ZXN0VXNlciBmcm9tIHdpbmRvdyIsInRzIjoxNDgxMjc1MDU1ODE1LCJ2ciI6IjEuMy4zIiwiZGV2aWNlIjp7Im5hbWUiOiJwYyJ9LCJzeXN0ZW0iOnsibmFtZSI6IndpbmRvd3MiLCJ2ZXJzaW9uIjoiNi4xIn0sImJyb3dzZXIiOnsibmFtZSI6ImNocm9tZSIsInZlcnNpb24iOiI1NC4wLjI4NDAuOTkifSwic2NyZWVuIjoiMTkyMCoxMDgwIiwidXNlcm1hcCI6eyJzYWEiOiJmcm9tIHdpbmRvdyJ9fQ%3D%3D"
    line = '''127.0.0.1,-,[20/Mar/2017:23:59:02 +0800],"POST /h5sta.gif?params=eyJhcHBrZXkiOiJuY2Zfd3MiLCJ0eXBlIjoicGFnZSIsInN1cHBvcnQiOiIyMiIsInVyaSI6Imh0dHBzOi8vbS5uaWNhaWZ1LmNvbS91c2VyL2Z1bmQvZGV0YWlsP2NvZGU9NTkwMDA4JnR5cGU9aG9sZGluZyIsInJlZiI6IiIsInVpZCI6IjE0OTAwMjU0ODg1MTNfOHplOGJnNDY3NCIsInB1c2hpZCI6Ijk1OGRhN2YxMDNiYjE4MDdkNzdkMTlkMTY2YmYzYjVlIiwidHMiOjE0OTAwMjU1NDM2ODksInZyIjoiMS4zLjUiLCJkZXZpY2UiOnsibmFtZSI6Im1pIiwidmVyc2lvbiI6IjRjIn0sInN5c3RlbSI6eyJuYW1lIjoiQW5kcm9pZCIsInZlcnNpb24iOiI1LjEuMSJ9LCJicm93c2VyIjp7Im5hbWUiOiJjaHJvbWUiLCJ2ZXJzaW9uIjoiNTUuMC4yODgzLjkxIn0sInNjcmVlbiI6IjM2MCo2NDAifQ%3D%3D HTTP/1.1",-,200 43,"https://m.nicaifu.com/user/fund/detail?code=590008&type=holding","Mozilla/5.0 (Linux; Android 5.1.1; Mi-4c Build/LMY47V; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/55.0.2883.91 Mobile Safari/537.36 Yourwealth uuid/4d27cf0f80a1bc6cd512d5b332e4bc30 udevice/android uversion/1.7.0 channel/ncf_10005 Yourwealth uuid/4d27cf0f80a1bc6cd512d5b332e4bc30 udevice/android uversion/1.7.0 channel/ncf_10005 Yourwealth uuid/4d27cf0f80a1bc6cd512d5b332e4bc30 udevice/android uversion/1.7.0 channel/ncf_10005" "36.63.40.63"'''
    tester = Transform()
    print tester.transform(line)