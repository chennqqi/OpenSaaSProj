# from pyminifier.obfuscate import apply_obfuscation
# from pyminifier.obfuscate import obfuscation_machine
#
#
# name_generator = obfuscation_machine(use_unicode=True)
# apply_obfuscation.name_generator = name_generator
import os
import requests
import json


def GetObfuscation(path, mode="pyminifier"):
    # print os.getcwd(), path
    if mode == "pyminifier":
        r = os.popen("pyminifier -O --replacement-length=50 %s"%path)
        # r = os.popen("pyminifier --nonlatin --replacement-length=50 %s"%path)
        text = r.read()
        r.close()
    elif mode == "pyob.oxyry.com":
        text = ObfuscatedCodeOnline(path)
    return text

def ObfuscatedCodeOnline(path):
    _code = open(path).read()
    url = "http://pyob.oxyry.com/obfuscate"
    data = {}
    data.setdefault("remove_docstrings", True)
    data.setdefault("rename_default_parameters", False)
    data.setdefault("rename_imports", True)
    data.setdefault("rename_nondefault_parameters", True)
    data.setdefault("source", _code)
    r = requests.post(url, json=data)
    text = r.text
    result = json.loads(text)
    return result["dest"]

if __name__ == "__main__":
    path = "C:/PycharmProjects/transform/TransformDeCipher/Obfuscation/testcode.py"
    GetObfuscation(path)

