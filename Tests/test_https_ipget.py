# -*- coding: utf-8 -*-
import requests



AK = 'paKQrVfPli2c5emzQ7nEtpEskqOitVDf'
ip = '220.175.40.224'

headers = {
"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
"Accept-Encoding": "gzip, deflate, sdch, br",
"Accept-Language": "zh-CN,zh;q=0.8",
"Cache-Control": "no-cache",
"Connection": "keep-alive",
"Cookie": "__cfduid=d13147f620dc5152755a9b0a44b319b0f1463628787; BAIDUID=892D8AEEAE60F60EBF1461280DA77058:FG=1; PSTM=1475061835; BIDUPSID=20962A615DE96EFFA8D6F7AD9C592013; BDUSS=WotNlJ4SkxtUjA1dnNmVFdJdndFUWljcGNzdG5PcUl-Vy1SbmdLWk9FWGNJU0JZQVFBQUFBJCQAAAAAAAAAAAEAAAA2REwsc2R6NzEyMTIxMQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAANyU-FfclPhXU; MCITY=-131%3A; BDSFRCVID=jHksJeC62ATd9fjiDBmr-qe_heWCIiOTH6aod6NjUse8t3gSThmyEG0Pqf8g0Ku-KzNqogKK0mOTHUvP; H_BDCLCKID_SF=tRk8oCL2JDvbfP0kKJo_-JjH-UnLqM6LfT7Z0lOnMp05MR3EhPrU3pvbXUFt-lRXBenyK-o7-JvW8CO_e6tKj53XjG8s-bbfHDvjQKthtTrjDCvFy-R5y4LdjG5CLP085DQ0KpoE-j6aef7dbqJTytFg3-Aq54RbJeoB-4OHfP-5eUO2M-cBQfbQ0hQhqP-jW5ILa-ck-R7JOpkxhfnxyhLB0a62btt_JRFtVUK; PSINO=2; H_PS_PSSID=1446_21097_17001_21311_21554_21593",
"Host": "api.map.baidu.com",
"Pragma": "no-cache",
"Upgrade-Insecure-Requests": 1,
"User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36"}

url_format = "https://api.map.baidu.com/location/ip?ak=%(ak)s&coor=bd09ll&ip=%(ip)s"

url = url_format % {"ip": ip, "ak": AK}
print url
# r = requests.get(url, headers)
r = requests.get(url)
print r.status_code
print r.text
