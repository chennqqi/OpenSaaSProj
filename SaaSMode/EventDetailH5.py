# -*- coding: utf-8 -*-
import uuid
import datetime
import json
from BasicMode import BasicMode

class EventDetailH5(BasicMode):
    def __init__(self):
        self._id = str(uuid.uuid1())
        self._appkey = ""
        self._type = ""
        self._status = ""
        self._value = 0L
        self._uri = ""
        self._uid = "-1"
        self._opatime = datetime.datetime.now()
        self._vr = ""
        self._device = None
        self._system = None
        self._browser = None
        self._screen = ""
        self._ua = ""
        self._ts = datetime.date.today()
        self._support = ""
        self._usermap = None
        self._ref = ""
        self._event = ""

    @property
    def appkey(self):
        return self._appkey

    @appkey.setter
    def appkey(self, appkey):
        self._appkey = appkey.strip()

    @property
    def type(self):
        return self._type

    @type.setter
    def type(self, type):
        self._type = type.strip()

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, status):
        self._status = status.strip()

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        self._value = value

    @property
    def uri(self):
        return self._uri

    @uri.setter
    def uri(self, uri):
        self._uri = uri.strip()

    @property
    def uid(self):
        return self._uid

    @uid.setter
    def uid(self, uid):
        self._uid = uid.strip()

    @property
    def opatime(self):
        return self._opatime

    @opatime.setter
    def opatime(self, opatime):
        self._opatime = opatime

    @property
    def vr(self):
        return self._vr

    @vr.setter
    def vr(self, vr):
        self._vr = vr.strip()

    @property
    def device(self):
        return self._device

    @device.setter
    def device(self, device):
        self._device = device

    @property
    def system(self):
        return self._status

    @system.setter
    def system(self, system):
        self._system = system

    @property
    def browser(self):
        return self._browser

    @browser.setter
    def browser(self, browser):
        self._browser = browser

    @property
    def screen(self):
        return self._screen

    @screen.setter
    def screen(self, screen):
        self._screen = screen.strip()

    @property
    def ua(self):
        return self._ua

    @ua.setter
    def ua(self, ua):
        self._ua= ua.strip()

    @property
    def ts(self):
        return self._ts

    @ts.setter
    def ts(self, ts):
        self._ts = ts

    @property
    def support(self):
        return self._support

    @support.setter
    def support(self, support):
        self._support = support.strip()

    @property
    def usermap(self):
        return self._usermap

    @usermap.setter
    def usermap(self, usermap):
        self._usermap = usermap

    @property
    def ref(self):
        return self._ref

    @ref.setter
    def ref(self, ref):
        self._ref = ref.strip()

    @property
    def event(self):
        return self._event

    @event.setter
    def event(self, event):
        self._event = event.strip()

    def build(self):
        return (self._id,
                self._appkey,
                self._type,
                self._status,
                self._value,
                self._uri,
                self._uid,
                self._opatime,
                self._vr,
                json.dumps(self._device, ensure_ascii=False),
                json.dumps(self._system, ensure_ascii=False),
                json.dumps(self._browser, ensure_ascii=False),
                self._screen,
                self._ua,
                self._ts,
                self._support,
                json.dumps(self._usermap, ensure_ascii=False),
                self._ref,
                self._event)

    def builder(self):
        return self.build()

    def merge(self, _old, _new = None):
        pass
