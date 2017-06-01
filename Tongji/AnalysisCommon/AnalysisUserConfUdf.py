# -*- coding: utf-8 -*-
from Tongji.AnalysisMap.AnalysisMap import AnalysisMap
import json
import time
from DBClient.MysqlClient import MysqlClient

class AnalysisUserConfUdf(AnalysisMap):

    def __init__(self):
        self.finished = False

    def rules(self, analysisresult, data, num, *args, **kwargs):
        self.finished = True
        self.dbname = kwargs["dbname"]
        self.plat = kwargs["plat"]
        self.datatype = kwargs["datatype"]

    def transformresult(self, analysisresult, *args, **kwargs):
        client = MysqlClient(self.dbname)
        data = client.select("select id, name, isdel from %(dbname)s.%(datatype)s_%(plat)s_eventid_udf" % {
            "dbname": self.dbname,
            "datatype": self.datatype,
            "plat": self.plat,
        })
        result = analysisresult.result
        for item in data:
            _id, name, isdel = item[0], item[1], item[2]
            result.setdefault(_id, [name, isdel])
        # result = analysisresult.result
        analysisresult.transformresult = result

        client.closeMysql()

