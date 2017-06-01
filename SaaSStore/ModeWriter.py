# -*- coding: utf-8 -*-
from abc import ABCMeta
from abc import abstractmethod


class ModeWriter:
    __metaclass__ = ABCMeta

    @abstractmethod
    # def write(self, data, appkey, modename, modetools, *args, **kwargs):
    def write(self, *args, **kwargs):
        pass

    @abstractmethod
    def remove(self, *args, **kwargs):
        pass

    def store_attachmode(self, data, appkey, modename, modetools, *args, **kwargs):
        if not hasattr(self, "attachmode_storers"):
            self.attachmode_storers = []
        for storer in self.attachmode_storers:
            storer.write(data, appkey, modename, modetools, *args, **kwargs)