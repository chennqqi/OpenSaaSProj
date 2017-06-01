# -*- coding: utf-8 -*-
from LogController import LogControler
from SaaSMode.UserMapMetaBuilder import UserMapMetaBuilder
from SaaSTools.tools import find_value_fromdict_singlekey
from SaaSCommon.JHOpen import JHOpen
from SaaSConfig.config import get_file_path
import json


class UserMapMeta(LogControler):

    def __init__(self, datatype, yyyy_mm_dd, hhmm="2359", last=1440):
        self.datatype = datatype
        self.yyyy_mm_dd = yyyy_mm_dd
        self.yyyymmdd = yyyy_mm_dd.replace("-", "")
        self.paths = get_file_path(datatype=self.datatype, yyyymmdd=self.yyyymmdd, hhmm=hhmm, last=last)
        self.userDict = {}
        # 配置需要收集的mapkey
        self.enum_config = {
            "biqu": [],
            "BIQU_ANDROID": [],
            "biqu_all": []
        }

    def setPaths(self, paths):
        self.paths = paths
        # self.userDict = BigDict()

    def pipeline(self, path):
        for line in JHOpen().readLines(path):
            if not line:
                continue
            data = json.loads(line)
            yield [data]

    def parse(self, data, entities, mode=UserMapMetaBuilder):
        try:
            optype = find_value_fromdict_singlekey(data, "jhd_opType")
            if optype != "action":
                return
            eventid = find_value_fromdict_singlekey(data, "jhd_eventId")
            entity = self.userDict[eventid] if eventid in self.userDict else mode(eventid)
            jhmap = find_value_fromdict_singlekey(data, "jhd_map")
            if not isinstance(jhmap, dict):
                return
            for key in jhmap:
                entity.addField(key, jhmap[key], self.enum_config.get(self.datatype, None))
            if eventid not in self.userDict:
                entities.append(entity.build())
                self.userDict.setdefault(eventid, entity)
            return entity
        except:
            import traceback
            print(traceback.print_exc())

    def dataCollect(self, *args, **kwargs):
        entities = []
        for path in self.paths:
            for line in self.pipeline(path):
                if not line:
                    continue
                try:
                    data = line[0]
                    self.parse(data, entities)
                except:
                    import traceback
                    print(traceback.print_exc())
        return entities

