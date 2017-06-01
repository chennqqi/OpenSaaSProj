# -*- coding: utf-8 -*
import re

ip_pattern = re.compile(r'''"((?:(?:25[0-5]|2[0-4]\d|((1\d{2})|([1-9]?\d)))\.){3}(?:25[0-5]|2[0-4]\d|((1\d{2})|([1-9]?\d))))"''')


def test(line):
    ip = line.split(",")[0].strip()
    a = ip_pattern.search(line)
    print ip_pattern.search(line).group(1)
    print ip_pattern.search(line).group(0)
    print type(a.group(0)), a.group(0)
    if ip.startswith("127"):
        for match_item in ip_pattern.findall(line):
            print match_item


line = '''127.0.0.1,-,[22/Sep/2016:09:16:34 +0800],"GET /h5sta.js?params=eyJhcHBrZXkiOiJCUV9INSIsInR5cGUiOiJkdXIiLCJzdGF0dXMiOiJlbmQiLCJ2YWx1ZSI6OTQ1LCJ1cmkiOiJodHRwOi8vMTkyLjE2OC4xLjY4Ojg5NTAvYXBwL2lubGFuZENhYmluTWVzc2FnZT9pc0JhY2s9Z28iLCJ1aWQiOiJsYiIsInRzIjoxNDc0NTA2OTk0MjAzLCJ2ciI6IjEuMy4yIiwiZGV2aWNlIjp7Im5hbWUiOiJpcGhvbmUiLCJ2ZXJzaW9uIjoiLTEifSwic3lzdGVtIjp7Im5hbWUiOiJpUGhvbmUgT1MiLCJ2ZXJzaW9uIjoiOS4xIn0sImJyb3dzZXIiOnsibmFtZSI6Ind4IiwidmVyc2lvbiI6IjYuMy45In0sInNjcmVlbiI6IjQxNCo2OTYiLCJ1YSI6Ik1vemlsbGEvNS4wIChpUGhvbmU7IENQVSBpUGhvbmUgT1MgOV8xIGxpa2UgTWFjIE9TIFgpIEFwcGxlV2ViS2l0LzYwMS4xLjQ2IChLSFRNTCwgbGlrZSBHZWNrbykgVmVyc2lvbi85LjAgTW9iaWxlLzEzQjE0MyBTYWZhcmkvNjAxLjEgd2VjaGF0ZGV2dG9vbHMvMC43LjAgTWljcm9NZXNzZW5nZXIvNi4zLjkgTGFuZ3VhZ2UvemhfQ04gd2Vidmlldy8wIn0%3D HTTP/1.1",-,200 13,"http://192.168.1.68:8950/app/inlandCabinMessage?isBack=go","Mozilla/5.0 (iPhone; CPU iPhone OS 9_1 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Version/9.0 Mobile/13B143 Safari/601.1 wechatdevtools/0.7.0 MicroMessenger/6.3.9 Language/zh_CN webview/0" "125.34.15.99"'''

test(line)