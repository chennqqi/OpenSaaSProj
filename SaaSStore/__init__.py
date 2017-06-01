# -*- coding: utf-8 -*-
# 解析日志入库
# from SaaSCommon.JHWrite import JHWrite
# from SaaSConfig.config import get_uvfile_path
# from DBClient.PyMongoClient import PyMongoClient
# from DBClient.MysqlClient import MysqlClient
from os import path, sys
import os
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
sys.path.append(path.dirname(path.dirname(path.dirname(path.abspath(__file__)))))
configPath = os.sep.join([path.dirname(path.abspath(__file__)), "Config.ini"])