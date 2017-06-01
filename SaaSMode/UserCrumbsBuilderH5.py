# -*- coding: utf-8 -*-
from UserCrumbsBuilder import UserCrumbsBuilder


class UserCrumbsBuilderH5(UserCrumbsBuilder):

    def __init__(self):
        super(UserCrumbsBuilderH5, self).__init__()

    def setSupport(self, support):
        self._user["support"] = support

    def setDevice(self, device):
        self._user["device"] = device

    def setBrowser(self, browser):
        self._user["browser"] = browser

    def setSystem(self, system):
        self._user["system"] = system