# -*- coding: utf-8 -*-
from Tongji.AnalysisMap.AnalysisMap import AnalysisMap
from SaaSTools.tools import getWeekDays


class AnalysisPushWeek(AnalysisMap):

    def __init__(self):
        super(AnalysisPushWeek, self).__init__()
        self.titles = [
                        u"P2P对账单",
                        u"自动对账单已出",
                        u"每月账单日",
                        u"房贷还款日",
                        u"信用卡还款日",
                        u"回款到期",
                        u"欠款到期",
                        u"定投日",
                        u"公积金存缴日",
                        u"有一笔资产到期",
                        u"借款到期",
                        u"到期提醒",
        ]

    def group_title(self, title):
        for _title in self.titles:
            if _title in title:
                return _title
        return title

    def rules(self, analysisresult, data, num, *args, **kwargs):
        yyyymmdd = kwargs['yyyymmdd']
        result = analysisresult.result
        try:
            event_id = data['jhd_eventId'].strip()
        except:
            return False
        uid = data['jhd_userkey'].strip()
        optime = data['jhd_opTime'].strip()
        opts = int(float(data['jhd_ts'].strip()))

        # 排除非当天日志
        if not optime.startswith(yyyymmdd):
            return False

        # 排除推送到达
        if event_id == "ac57":
            return False
        # 记账
        if event_id == "ac30":
            result.setdefault("ac30_user", {}).setdefault(uid, set()).add(opts)
            result.setdefault("all_user", {}).setdefault(uid, set()).add(opts)
        # 查账
        elif event_id == "ac10":
            result.setdefault("ac10_user", {}).setdefault(uid, set()).add(opts)
            result.setdefault("all_user", {}).setdefault(uid, set()).add(opts)
        # 推送
        elif event_id == "ac58":
            user_map = data["jhd_map"]
            title = self.group_title(user_map["title"])
            result.setdefault("push_user", {}).setdefault(uid, {}).setdefault(title, set()).add(opts)
        else:
            result.setdefault("all_user", {}).setdefault(uid, set()).add(opts)

    def transformresult(self, analysisresult, *args, **kwargs):
        raw_data = analysisresult.result

        all_user = raw_data.pop("all_user", {})
        push_user = raw_data.pop("push_user", {})
        ac30_user = raw_data.pop("ac30_user", {})
        ac10_user = raw_data.pop("ac10_user", {})
        result = {}
        # 推送打开人数、推送唤醒人数(推送打开前10分钟没有日志)、推送打开并记账（ac30）、推送打开并查账（ac10）
        for uid in push_user:
            for title in push_user[uid]:
                result.setdefault(title, [set(), set(), set(), set()])
                # 推送打开人数
                result[title][0].add(uid)
                push_ts = min(push_user[uid][title])
                all_ts = all_user.get(uid, set())
                # 推送唤醒人数(推送打开10分钟前没有日志)
                # if not (any(map(lambda x: 0<(push_ts-x)<60*10, all_ts))):
                if len([x for x in all_ts if (x-push_ts)<=-60*10*1000]) == 0:
                    result[title][1].add(uid)
                # 推送打开并记账
                ac30_ts = ac30_user.get(uid, None)
                if ac30_ts and any(map(lambda x: 0<(x-push_ts)<60*30*1000, ac30_ts)):
                    result[title][2].add(uid)
                # 推送打开并查账
                ac10_ts = ac10_user.get(uid, None)
                if ac10_ts and any(map(lambda x: 0<(x-push_ts)<60*30*1000, ac10_ts)):
                    result[title][3].add(uid)
        result_tmp = {}
        for title in result:
            result_tmp[title] = map(len, result[title])
        analysisresult.transformresult = result_tmp








