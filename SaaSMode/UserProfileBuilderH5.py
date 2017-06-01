# -*- coding: utf-8 -*-
from UserProfileBuilder import UserProfileBuilder


class UserProfileBuilderH5(UserProfileBuilder):

    def __init__(self):
        super(UserProfileBuilderH5, self).__init__()

    def setSupport(self, support):
        self._user["support"] = support

    def setDevice(self, device):
        self._user["device"] = device

    def setBrowser(self, browser):
        self._user["browser"] = browser

    def setSystem(self, system):
        self._user["system"] = system