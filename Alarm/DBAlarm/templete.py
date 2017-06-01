# coding: utf-8
# urllib.quote(json.dumps('''{"msg": {"content": "%(content)s", "item": u"%(title)s", "rec_num": "18612267006"}, "ts": "%(ts)s", "funcName": "SMS_13620007"}'''))

# 固定字段：
# msg: 用于指定报警模板方法中需要的信息
# ts：时间戳，表示报警发生的时间点
# funcName：表示需要调用的报警模板方法


templetes = {
    "default": '''{"msg": {"content": "%(content)s", "item": "%(title)s", "rec_num": "%(rec_num)s"}, "ts": "%(ts)d", "funcName": "SMS_13620007"}''',
}