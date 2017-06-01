# -*- coding: utf-8 -*-
from datetime import date
from pony.orm import *


def define_charge_overall_custom(db):

    class Charge_overall_custom(db.Entity):
        id = PrimaryKey(int, sql_type="int(11)", auto=True)
        tm = Optional(date)
        pub = Optional(str)
        ver = Optional(str)
        uv = Optional(int)  # 访客
        reg = Optional(int)  # 注册访客
        charge = Optional(int)  # 投资访客
        today_reg = Optional(int)  # 今日注册
        today_charge = Optional(int)  # 今日投资
        first_charge = Optional(int)  # 首投
        reg_charge = Optional(int)  # 今日注册首投
        recharge = Optional(int)  # 复投
        charge_7days = Optional(int)  # 7日内注册首投
        checkfin = Optional(int)  # (新注册)查看理财产品
        auth = Optional(int)  # (新注册)认证流程
        cert = Optional(int)  # (新注册)实名通过
        bankcard = Optional(int)  # (新注册)银行卡绑定
        charge_newer = Optional(int)  # (新注册)银行卡绑定
        dig = Optional(int)  # 挖宝参与
        dig_uc = Optional(int)  # 用户中心挖宝
        dig_homepage = Optional(int)  # 首页挖宝
        dig_jump = Optional(int)  # 挖宝后跳转
        dig_cur = Optional(int)  # 挖宝活期
        dig_curpay = Optional(int)  # 挖宝活期付款
        uc_invite = Optional(int)  # 用户中心邀请
        check_ucfinc = Optional(int)  # 查看个人中心理财产品或活动
        paypage = Optional(int)  # 付款页
        charge_success = Optional(int)  # 投资成功

    return Charge_overall_custom