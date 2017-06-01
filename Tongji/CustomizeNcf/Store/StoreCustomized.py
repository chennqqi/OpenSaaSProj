# -*- coding: utf-8 -*-
import __init__
from collections import OrderedDict
from Tongji.CustomizeNcf.Mode.ChargeOverall import define_charge_overall_custom as ChargeOverallCustom


from Tongji.CustomizeNcf.Analysis import AnalysisChargeOverall

from Tongji.AnalysisCommon.AnalysisResult import AnalysisResult
from pony.orm import *

from Tongji.StoreCustom.StoreBasicCustom import StoreBasicCustom
from datetime import date
from datetime import timedelta

class StoreCustomized(StoreBasicCustom):

    def __init__(self, store_freq = "daily"):
        # 名称、分析函数、存储函数、database对象、tableobject（ORM）
        if store_freq == "daily":
            self.storers = OrderedDict([
                # ("overall", self.store_overall),
                (("charge_overall_custom", "uvfile"), [AnalysisChargeOverall.AnalysisChargeOverall(), self.store_charge_overall, AnalysisResult(), ChargeOverallCustom]),
            ])
        else:
            self.storers = OrderedDict([])

    def analysisresult_clear(self):
        for key in self.storers:
            if isinstance(self.storers[key][2], AnalysisResult):
                self.storers[key][2].cleardata()

    # @fn_timer
    def store_charge_overall(self, result, num, dbname, datatype, plat, mainname="charge_overall_custom", logtype="uvfile", ifdel=False):
        tm = date.today() - timedelta(days=num)
        db, table = self.db_table(dbname, datatype, plat, mainname, logtype)
        with db_session:
            if ifdel:
                self.dataclear(num, table)
            for key in result:
                try:
                    pub, ver = key[0], key[1]
                    # 1、uv 访客, 2、reg 注册访客, 3、charge 投资访客, 4、today_reg 今日注册, 5、today_charge 今日投资,
                    # 6、first_charge 首投, 7、reg_charge 今日注册首投, 8、recharge 复投, 9、charge_7days 7日内注册首投,
                    # 10、checkfin (新注册)查看理财产品, 11、auth (新注册)认证流程, 12、cert (新注册)实名通过,
                    # 13、bankcard (新注册)银行卡绑定, 14、charge_newer (新注册)投资, 15、dig 挖宝参与,
                    # 16、dig_uc 用户中心挖宝, 17、dig_homepage 首页挖宝,, 18、dig_jump 挖宝后跳转, 19、dig_cur 挖宝活期,
                    # 20、dig_curpay 挖宝活期付款, 21、uc_invite 用户中心邀请, 22、check_ucfinc 查看个人中心理财产品或活动,
                    # 23、paypage 付款页, 24、charge_success 投资成功
                    uv = result[key][0]
                    reg = result[key][1]
                    charge = result[key][2]
                    today_reg = result[key][3]
                    today_charge = result[key][4]
                    first_charge = result[key][5]
                    reg_charge = result[key][6]
                    recharge = result[key][7]
                    charge_7days = result[key][8]
                    checkfin = result[key][9]
                    auth = result[key][10]
                    cert = result[key][11]
                    bankcard = result[key][12]
                    charge_newer = result[key][13]
                    dig = result[key][14]
                    uc_invite = result[key][15]
                    dig_uc = result[key][16]
                    dig_homepage = result[key][17]
                    dig_jump = result[key][18]
                    dig_cur = result[key][19]
                    dig_curpay = result[key][20]
                    check_ucfinc = result[key][21]
                    paypage = result[key][22]
                    charge_success = result[key][23]
                    table(tm=tm, ver=ver, pub=pub, uv=uv, reg=reg, charge=charge, today_reg=today_reg,
                          today_charge=today_charge, first_charge=first_charge, reg_charge=reg_charge, recharge=recharge,
                          charge_7days=charge_7days, checkfin=checkfin, auth=auth, cert=cert, bankcard=bankcard,
                          charge_newer=charge_newer, dig=dig, dig_uc=dig_uc, dig_homepage=dig_homepage, dig_jump=dig_jump,
                          dig_cur=dig_cur, dig_curpay=dig_curpay, uc_invite=uc_invite, check_ucfinc=check_ucfinc,
                          paypage=paypage, charge_success=charge_success)
                except:
                    import traceback
                    print(traceback.print_exc())
        db.disconnect()

if __name__ == "__main__":
    tester = StoreCustomized()
    print(tester.print_storers())

