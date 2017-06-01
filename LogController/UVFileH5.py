# -*- coding: utf-8 -*-
from UVFile import UVFile
from TransformH5toApp import TransformH5toApp
from SaaSCommon.JHOpen import JHOpen


class UVFileH5(UVFile):

    def __init__(self, datatype, yyyy_mm_dd, hhmm="2359", last=1440):
        super(UVFileH5, self).__init__(datatype, yyyy_mm_dd, hhmm, last)

