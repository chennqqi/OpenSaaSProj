# -*- coding: utf-8 -*-
import __init__
import time
import datetime
import itertools
from Tongji.AnalysisMap.AnalysisMap import AnalysisMap


class AnalysisChargeOverall(AnalysisMap):

    def __init__(self):
        super(AnalysisChargeOverall, self).__init__()
        self.finished = False

    # 计算新增、活跃、启动、PV（分版本/渠道）
    def rules(self, analysisresult, data, num, *args, **kwargs):
        result = analysisresult.result
        yyyymmdd = time.strftime("%Y%m%d", time.localtime(time.time() - 86400 * num))
        date = datetime.datetime.strptime(yyyymmdd, "%Y%m%d").date()
        plat, ver = "all", "all"
        # 1、uv 访客, 2、reg 注册访客, 3、charge 投资访客, 4、today_reg 今日注册, 5、today_charge 今日投资,
        # 6、first_charge 首投, 7、reg_charge 今日注册首投, 8、recharge 复投, 9、charge_7days 7日内注册首投,
        # 10、checkfin (新注册)查看理财产品, 11、auth (新注册)认证流程, 12、cert (新注册)实名通过,
        # 13、bankcard (新注册)银行卡绑定, 14、charge_newer (新注册)投资, 15、dig 挖宝参与,
        # 16、dig_uc 用户中心挖宝, 17、dig_homepage 首页挖宝,, 18、dig_jump 挖宝后跳转, 19、dig_cur 挖宝活期,
        # 20、dig_curpay 挖宝活期付款, 21、uc_invite 用户中心邀请, 22、check_ucfinc 查看个人中心理财产品或活动,
        # 23、paypage 付款页, 24、charge_success 投资成功
        result.setdefault((plat, ver), [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
        try:
            items = data.split("\t")
            # pushid = items[0].strip()
            # uids = items[1].strip().split("#")
            reg_time = items[2].strip()
            charge_days = int(items[3].strip())
            charge_time_batch = items[5].strip()
            # first_charge_time = min(charge_time_batch.split("#"))
            events = items[7].strip().split("#")
            uris_batch = items[9].strip()
            ### 访客 ###
            result[(plat, ver)][0] += 1
            ### 注册访客 ###
            if reg_time != "#":
                # 注册访客
                result[(plat, ver)][1] += 1
                ### 新注册访客 ###
                if reg_time.startswith(yyyymmdd):
                    # 今日注册
                    result[(plat, ver)][3] += 1
                    if "/detail" in uris_batch or "/dt_detail" in uris_batch or "dq/index?code=" in uris_batch:
                        # (新注册)查看理财产品
                        result[(plat, ver)][9] += 1
                    if "verify" in uris_batch:
                        # (新注册)认证流程
                        result[(plat, ver)][10] += 1
                    if "h5_reg_sm_next" in events:
                        # (新注册)实名通过
                        result[(plat, ver)][11] += 1
                    if "h5_reg_bk_next" in events:
                        # (新注册)银行卡绑定
                        result[(plat, ver)][12] += 1
                    if "success" in uris_batch:
                        # (新注册)银行卡绑定
                        result[(plat, ver)][13] += 1
            ### 投资 ###
            if charge_time_batch != "#":
                # 投资访客
                result[(plat, ver)][2] += 1
                if "https://wx.nicaifu.com/wx/wa" in uris_batch:
                    # 挖宝参与
                    result[(plat, ver)][14] += 1
                if "https://wx.nicaifu.com/wx/wa?from=gerenzhongxin#/invite" in uris_batch:
                    # 用户中心邀请
                    result[(plat, ver)][15] += 1
                if "https://wx.nicaifu.com/wx/wa?from=h5_grzxhuodong_wb#/" in uris_batch:
                    # 用户中心挖宝
                    result[(plat, ver)][16] += 1
                if "https://wx.nicaifu.com/wx/wa?from=h5_gongge1_wb#/" in uris_batch:
                    # 首页挖宝
                    result[(plat, ver)][17] += 1
                if "https://wx.nicaifu.com/wx/wa#/" in uris_batch:
                    # 挖宝后跳转
                    result[(plat, ver)][18] += 1
                if "https://wx.nicaifu.com/hq" in uris_batch:
                    # 挖宝活期
                    result[(plat, ver)][19] += 1
                if "https://wx.nicaifu.com/pay/newVersionBuy?" in uris_batch:
                    # 挖宝活期付款
                    result[(plat, ver)][20] += 1
                if "https://m.nicaifu.com/dq/index?code=" in uris_batch:
                    # 查看个人中心理财产品或活动
                    result[(plat, ver)][21] += 1
                if any(itertools.imap(lambda item: item in uris_batch,
                        ["pay/newVersionBuy", "pay/gs_buy", "payV2/buy", "ins/buy", "ins/pay"])
                        # ["pay/newVersionBuy"])
                       ):
                    # 付款页
                    result[(plat, ver)][22] += 1
                if any(itertools.imap(lambda item: item in uris_batch,
                        ["/success", "_success"])
                        # ["pay/newVersionBuy", "success"])
                       ):
                    if "".join(["#", yyyymmdd]) in charge_time_batch or charge_time_batch.startswith(yyyymmdd):
                        # 投资成功
                        result[(plat, ver)][23] += 1

                ### 今日投资访客 ###
                if "".join(["#", yyyymmdd]) in charge_time_batch or charge_time_batch.startswith(yyyymmdd):
                # if yyyymmdd in charge_time_batch:
                    # 今日投资
                    if any(itertools.imap(lambda item: item in uris_batch,
                                          ["/success", "_success"])
                           # ["pay/newVersionBuy", "success"])
                           ):
                        result[(plat, ver)][4] += 1
                        if charge_days == 1:
                            # 首投
                            result[(plat, ver)][5] += 1
                            if reg_time.startswith(yyyymmdd):
                                # 今日注册首投
                                result[(plat, ver)][6] += 1
                            if (date - datetime.datetime.strptime(reg_time[:8], "%Y%m%d").date()).days <= 7:
                                # 七日内注册首投
                                result[(plat, ver)][8] += 1
                        elif charge_days > 1:
                            # 复投
                            result[(plat, ver)][7] += 1
            return True
        except:
            import traceback
            print(traceback.print_exc())

    def transformresult(self, analysisresult, *args, **kwargs):

        result = analysisresult.result
        analysisresult.transformresult = result