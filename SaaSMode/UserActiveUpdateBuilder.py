# -*- coding: utf-8 -*-
from BasicMode import BasicMode

class UserActiveUpdateBuilder(BasicMode):

    def __init__(self):
        self._data = {}

    def setUserkey(self, userkey):
        if not (userkey and userkey != "null"):
            return
        self._data.setdefault("jhd_userkey", userkey.strip())

    def isNull(self):
        if self._data:
            return True
        else:
            return False

    def merge(self, _old, _new = None):
        _new = self._data if _new is None else _new
        return dict(_old, **_new)

    def builder(self):
        return self._data