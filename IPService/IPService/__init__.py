# -*- coding: utf-8 -*-
from os import sys, path
sys.path.append(path.dirname(path.abspath(__file__))) # 当前目录
sys.path.append(path.dirname(path.dirname(path.abspath(__file__)))) # 上级目录
# sys.path.append(path.dirname(path.dirname(path.dirname(path.abspath(__file__)))))
# sys.path.append(sys.path.append(path.dirname(path.dirname(path.dirname(path.abspath(__file__))))))

# import os
# file_name = os.path.join(path.dirname(path.abspath(__file__)), "".join([__file__, ".log"]))
# print file_name