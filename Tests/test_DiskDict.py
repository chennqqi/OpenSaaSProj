# -*- coding: utf-8 -*-

from SaaSCommon.BigDict import BigDict
from SaaSCommon.DiskDict import DiskDict


a = BigDict()
a = DiskDict(cache_name="777")

print "666", a
a["test"] = "test112"
a.update()