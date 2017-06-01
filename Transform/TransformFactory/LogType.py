# -*- coding: utf-8 -*-

class LogType(object):

    # @staticmethod
    def __init__(self):
        pass

    @staticmethod
    def type(log):


        if "datatype=" in log:
            return "dzh"
        elif "abc=" in log:
            return "saas"
        elif "params=" in log:
            return "h5"