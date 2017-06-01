# -*- coding: utf-8 -*-
from UserEventBuilder import UserEventBuilder


class UserEventBuilderH5(UserEventBuilder):

    def __init__(self):
        super(UserEventBuilderH5, self).__init__()

    def setSupport(self, support):
        self._event["support"] = support

    def setDevice(self, device):
        self._event["device"] = device

    def setBrowser(self, browser):
        self._event["browser"] = browser

    def setSystem(self, system):
        self._event["system"] = system