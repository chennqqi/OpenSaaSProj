# -*- coding: utf-8 -*-
from LogType import LogType
from TransformDeCipher import Transform as TransformSaas
from TransformH5 import Transform as TransformH5
from TransformSaasLog import Transform as TransformDZH


class Factory(object):

    # @staticmethod
    def __init__(self):
        pass

    @staticmethod
    def creater(log):
        logtype = LogType.type(log)
        print logtype
        if logtype == "dzh":
            return TransformDZH.Transform()
        elif logtype == "saas":
            return TransformSaas.Transform()
        elif logtype == "h5":
            return TransformH5.Transform()


if __name__ == "__main__":
    line = '''175.147.93.67,-,[22/Sep/2016:23:59:18 +0800],"GET /appsta.js?datatype=guaeng&os=iphone_9.30&vr=1.2.0818&pb=appstore&userkey=6E3AE090-B665-4987-B633-7B08FE972400&ua=iphone_5s_(a1457/a1518/a1528/a1530)&pushid=73ac84d0301623b9182a1d24d11d551a&isupdate=1&session=2016-09-22%2B23%3A59%3A18%23wifi%23page%23id%3D%E7%B1%B3%E8%8F%B2%E5%85%94_Miffy%2A%E7%B1%B3%E8%8F%B2%E7%B3%BB%E5%88%97%E8%8A%B1%E7%B5%AE_watch%24ref%3Dst&sver=V3.4&sig=12643a649dc4b016d59791d0ae7e1bc4 HTTP/1.1",-,204 0,"-","english_student/1.2.0818 (iPhone; iOS 9.3.5; Scale/2.00)" "-"'''
    tr = Factory.creater(line)
    for item in tr.transform(line):
        print item
