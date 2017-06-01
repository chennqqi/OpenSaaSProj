# -*- coding: utf-8 -*-
from SaaSCommon.Stack import Stack


class StackSet(Stack):

    def push(self, item):
        if item in self.items:
            return
        super(StackSet, self).push(item)