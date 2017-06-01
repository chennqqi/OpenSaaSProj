# -*- coding: utf-8 -*-
import re

ncf_360 = {
    "input_path_format": "/data1/logs/transformsaaslogs/ncf_ws/%(yyyymmdd)s/%(hhmm)s.log.gz",
    "out_path_format": "/data1/logs/transformsaaslogs/ncf_360/%(yyyymmdd)s/%(hhmm)s.log.gz",
    "filter_chain": [
        {
            'filter': lambda target: is_in('yourwealth/safe/agency', target),
        },
        {
            'filter': lambda target: is_in('mobilesafe wallet', target),
        },
        {
            'filter': lambda target: is_in('360WS', target),
        },
        {
            'filter': lambda target: is_in('yourwealth/safe/business', target),
        },
        {
            'filter': lambda target: is_in('yourwealth/safe/icon', target),
        },
        {
            'filter': lambda target: is_in('yourwealth/safe/banner', target),
        },
        {
            'filter': lambda target: is_match(re.compile('360ms[\w\W]+360webview'), target),
        },
        {
            'filter': lambda target: is_in('360Shake', target),
        },
        {
            'filter': lambda target: is_in('Lockscreen', target),
        },
        {
            'filter': lambda target: is_in('yourwealth/safe/exam', target),
        },
        {
            'filter': lambda target: is_in('yourwealth/safe/card', target),
        },
        {
            'filter': lambda target: is_in('360WS_Home', target),
        },
        {
            'filter': lambda target: is_in('360WS_Tools', target),
        },
        {
            'filter': lambda target: is_in('360WS_Service', target),
        },
        {
            'filter': lambda target: is_in('360WS_My', target),
        },
        {
            'filter': lambda target: is_in('360WS_YYY', target),
        },
        {
            'filter': lambda target: is_in('360WS_Wallet', target),
        },
        {
            'filter': lambda target: is_in('360WS_WidgetAds', target),
        },
        {
            'filter': lambda target: is_in('mobilesafe/business', target),
        },
        {
            'filter': lambda target: is_in('360WS_Notification', target),
        },
        {
            'filter': lambda target: is_in('360WS_Message', target),
        },
        {
            'filter': lambda target: is_in('360WS_QRCode', target),
        },
        {
            'filter': lambda target: is_in('360WS_Pendant', target),
        },
        {
            'filter': lambda target: is_in('android my service', target),
        },
        {
            'filter': lambda target: is_in('android clean', target),
        },
        {
            'filter': lambda target: is_in('android anti-virus', target),
        },
        {
            'filter': lambda target: is_in('hjzs gj', target),
        },
        # 没有实例日志，没办法测试匹配规则
        # {
        #     'filter': lambda target: is_in('360WS & 360Shake & from=sjws_jbsc', target),
        # },
    ],
}


def is_in(content, target_str):
    if content in target_str:
        return True
    else:
        return False


def is_match(pattern, target_str):
    for item in pattern.finditer(target_str):
        return True
    return False


if __name__ == "__main__":
    parttern = re.compile('360ms[\w\W]+360webview')
    line = "360ms_ 999ggafs_360webview"
    print is_match(parttern, line)