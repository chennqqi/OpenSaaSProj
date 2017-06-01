# -*- coding: utf-8 -*-
from BasicMode import BasicMode


class UserEventBuilder(BasicMode):

    def __init__(self):
        self._event = {}
        # self._event["eventmap"] = {"ct": "#"}

    # event
    def setOpType(self, optype):
        self._event["jhd_opType"] = optype if optype else "#"

    def setEventId(self, eventid):
        self._event["jhd_eventId"] = eventid if eventid else "#"

    # who
    def setUid(self, userkey):
        self._event["jhd_userkey"] = userkey if userkey else "#"

    def setPushid(self, pushid):
        self._event["jhd_pushid"] = pushid if pushid else "#"

    # when
    # def setCT(self, ct):
    #     self._event["jhd_ct"] = ct

    def setTS(self, ts):
        try:
            ts = long(int(float(ts)))
        except:
            import traceback
            print(ts, traceback.print_exc())
            return
        # assert isinstance(ts, int), "%s,%s"%(str(type(ts)), str(ts))
        self._event["jhd_ts"] = ts

    # where
    def setIP(self, ip):
        self._event["jhd_ip"] = ip if ip else "#"

    # def setLoc(self, loc):
    #     # format: prov_city_county
    #     self._event["jhd_loc"] = loc

    # how
    def setPub(self, pb):
        self._event["jhd_pb"] = pb if pb else "#"

    def setOS(self, os):
        os = os.replace(" ", "_") if os else "#"
        self._event["jhd_os"] = os if os else "#"

    def setNet(self, net):
        self._event["jhd_netType"] = net if net else "#"

    def setUA(self, ua):
        self._event["jhd_ua"] = ua if ua else "#"

    def setVR(self, vr):
        self._event["jhd_vr"] = vr if vr else "#"

    def setSdkVr(self, vr):
        self._event["jhd_sdkvr"] = vr if vr else "#"

    # what
    def setMap(self, eventmap):
        self._event["jhd_map"] = eventmap if isinstance(eventmap, dict) else {}

    def build(self):
        return self._event

    def builder(self):
        return self.build()

    def merge(self, _old, _new = None):
        pass