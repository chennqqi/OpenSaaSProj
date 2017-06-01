# -*- coding: utf-8 -*-
from BasicMode import BasicMode


class UserMapMetaBuilder(BasicMode):

    def __init__(self, _id):
        self._data = {
            "_id": _id,
            "fields": [],
            "field_elems": {}
        }

    def addField(self, fieldid, value, enum_elems = None):
        if enum_elems is None:
            enum_elems = []
        field_data = {}
        if isinstance(value, list):
            value_type = "array"
        elif isinstance(value, dict):
            value_type = "object"
        elif isinstance(value, unicode) or isinstance(value, str):
            value_type = "string"
        elif isinstance(value, int) or isinstance(value, float):
            value_type = "number"
        elif isinstance(value, bool):
            value_type = "bool"

        field_data.setdefault("name", fieldid)
        field_data.setdefault("type", value_type)
        if field_data not in self._data["fields"]:
            self._data["fields"].append(field_data)
        # if fieldid in enum_elems:
        if value and value not in self._data["field_elems"].get(fieldid, []):
            self._data["field_elems"].setdefault(fieldid, []).append(value)

    def build(self):
        return self._data

    def builder(self):
        return self.build()

    def merge(self, _old, _new = None):
        pass