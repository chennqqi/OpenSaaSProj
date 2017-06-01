# coding: utf-8
import IPtoAreaFinals
from __init__ import ipdataPath
# from SaaSConfig.config import ipdata_path
global initarry
initarry = None

def getLoc(ip):
    global initarry
    if initarry is None:
        initarry = IPtoAreaFinals.load(ipdataPath)
    return IPtoAreaFinals.getlocid(ip, initarry, type = "for_more")

# prov, city = IPtoAreaFinals.getlocid("8.8.8.8", initarry, type = "for_more")



if __name__ == "__main__":
    import os
    print(os.getcwd())
    loc = getLoc("8.8.8.8")
    print(loc[0], loc[1])
    print(type(loc), loc)
