# coding: utf-8
from Decipher import Decipher
import json
import urllib
import re
from TransformRule_dev import TransformRule
import sys
import time
reload(sys)
sys.setdefaultencoding("utf-8")

class Transform(object):

    def __init__(self, timestamp = time.time()):
        self.decipher = Decipher()
        self.timestamp = timestamp
        self.params_format = re.compile(unicode(r"\[.*]"))
        self.ip_pattern = re.compile(
            r'''"((?:(?:25[0-5]|2[0-4]\d|((1\d{2})|([1-9]?\d)))\.){3}(?:25[0-5]|2[0-4]\d|((1\d{2})|([1-9]?\d))))"''')
        self.rule = TransformRule()

    def transform(self, line):
        line = urllib.unquote_plus(line).decode("utf8")
        items = line.split(",")
        ip = items[0]
        # 如果为内网ip，做单独处理
        try:
            if ip.startswith("127"):
                ip = self.ip_pattern.search(line).group(1)
        except:
            # import traceback
            # print(traceback.print_exc())
            # print line
            pass
        response_ty = items[3]
        assert "post" in response_ty.lower(), 'isnot POST REQUEST'
        content = items[4]
        if "&params=" in content:
            p1, p2 = content.split("&")
            tipkey = p1.split("abc=")[1].strip()
            params_enauth = p2.split("params=")[1].strip()
            params = self._authLog(tipkey, params_enauth)
            auth_sw = 'on'
        else:
            params = urllib.unquote_plus(content.split("=")[1]).decode("utf8")
            auth_sw = 'off'
        for item in self.params_format.findall(params):
            params = item
            break
        for item in json.loads(params):
            item['auth'] = auth_sw
            item['ip'] = ip
            yield self.rule.applyRule(item, self.timestamp)

    def _authLog(self, tipkey, params):
        dtipkey = self.decipher.deCiphering(tipkey)
        dparams = self.decipher.deCiphering(params, dtipkey)
        return dparams

if __name__ == "__main__":
    tester_Transform = Transform()
    line = '''111.207.2.232,-,[03/Jan/2017:23:58:14 +0800],"POST /iossta.js HTTP/1.1",abc%3DmSUQ9D7ilbSaix%2Fa%2Bj5SlA%3D%3D&params%3DWm2NJpu8E1PWX%2FA8O4vtQTzIWKPtrZtwMqGIUrJCRA137IkSBF4p8QWY1PMk8Tljie3oeLRt%2B6B%2F80GQnxF%2Bqn9CtTtD7Hdw2W3Qr7q5N9zpYXM4kGYY4Zmb9tW9LnAqlR5JVcxZxj5bE4D8yKJhdSoF5LXqaCPV4Nx3jWpZvPQEU9Lk737v4SxVESaAACA0ee6OscrgyK0G6g36g%2B5vczuk9I4E%2FYcxydtiUUzrEpFC06HoL8fJXljXvJ5QzzAMTnfrPj3qU73gd%2BlYnq9zeYbJRlto29JxbIP7crXdk%2FI0JbXizCxx0oBbJ6%2Bj9E5NMwrk5UQgc2C7ir5GD8E7XIDaoOm7GEPcRo8CqDCtHXcI5wGLJil0idaSQnRtp25%2Fa%2F%2BmbdoOeOhitiz1bPi95y%2FWGdlzmi4UQCm1F6VH0eXudkjkpWEbqm05NXyeBO2%2BhQNtnhtFRenzbTu0RTvUUFcImWVOVpQMKnpYAXT4%2Fq5nVtPPqYncMEhdX0Ht4%2FKM89JdSEtJPrZYuqvINpgaJ%2BnNSo%2Fa8l%2B6tfVVlQlEzLzRX2Sf2Q8u5890dLfMfISGt9ONa051ADFcQRysMX1mNkwJYW3wrTAppmBD0qyYGxjD4Gjpsn5UyA6RyE767BeLqAYQFFC5KUskw%2BJqgd6xl0K47ytW2y7PEOvhvL4GcXE5JzqG1L%2Fx0I2b%2FZCiMh6DSR9B%2FaYy7WKEcMjzp%2Fkhqw7gvYJr8rYi%2FOc3gViMKM%2B86nRVZ1RuBrTTjZRMZBcHS8eX1F5Oukt2WpcSpm54LdtaV49l%2Beo%2BZA2rZvT5P6xPnn2wlqOtZNf%2BRkY3axyCvG6x3IC7V8XHf7zPLtuJ5eiC5Y193ideZRpcAd2lm7hUYYEh7rMq9vp%2BEjSJq9wiLokQx4L%2F4iKll0Za3hue2Pynn7FfRKcB9ioXB0FDShoptM5BF0Vt8UDoINfRnJaSzibG1aA4Rx3NQpfCHNed1fgJ6NllaswjO61sEXsNR1N0qrP2g6A2k1AznJIPhXpXw5gM9Ud5L1thBgBTugEweritMNa7G737TnkLuid4EQmpHS0dMJ%2BDxpCQKa5UEwTn6fQFmdrQDuqbtkg5Qc1c4fXAoAWIyVxFase8p767JBLZSXTvHe%2BRTr7Vou4FfB3yef%2B69oyKM%2BeCv0jM1%2FXk2aUIyYItZMjK0q9KaNgVoJjQcBnTDeQ%2FpmGq5vKg6PFFHpAB%2BDZcfz9Bq%2FCLsHBdV117ZqmaWdNTQrSUEFwqHl6wVe2WENGKK46PtObq09769c4p5ExIO2zlYQaMEstpOoGw7IsXRTo%2Fx%2ByRajLHlaqkn5ede4ja6shpgICPysl1HVfE5spMhIgWQja46Wd5s2AL%2B6tJ%2B4JvFb7%2B%2Feli5i0JjOMdZEzoO3dY%2BLvvybT9I%2BpDB0ogPF6YapUCbx%2FbvEyE0TmxBorjDBF9xXnL9QnkdjnbnWMD0I6tWMVHLmZpwr4rD8ATb8XLU1IH9r46Kn9Wmy0mwcWE5yF%2F4hDi1VvCxZ59VEFZ9plnPelcBjiC3bSog2SbqJW4Geyz5kBd6Z1Jj0BHwKRWnmjCz%2FZG4tvSMjRfuUb3MPHayyZkAEo2TAOORplOQru6xvDtpZG4M4TfEUlouny%2FeJiGrFsuNUCG4OUMH9v8RB%2BHUZSD8zdgkOj9NX6FqSnSPiHSnHUsmOCSkiUYFH8ABC4J86Rc9e%2F4Qg86TjnX44HZFRHi5nSvFyRqAtyuCWQMCBJqzguHoOlALXk55IKqzCcU6SV9sM0Z6iBA1pMF%2BqhnkV3pjU%2FvSIACdb2NXxtrWg%2B7rRo%2FB%2FjVnUYLy5%2B2cwy6bXCP%2F6jQyAson4w%2BIAqf7Sed5tAVLKTqQe06VE9Wls8Fp36M0THcrr4SsT6zM%2BLoSRRTImQUMEhxW3aBglzmyDKVV1RYEw76l17huSDj6e9gvaDBOFKA9HnMO3891VdqrLDUIuSVaEBEzMt%2F9GDQGZzTHng7poxZrkjLml35LAZdg9u9OzNp22zkpA7a99AkA6DvhntC8XNIxKQXKWOo5JglgM%2FwiS0yO4YanKRiOTWqpezPiY0lxSzSfJSqDm6BZIhXQuJRIYK5I1WVGeY1prc%2Bb30QGtRJLwolKGKjrXgNJvx9jyZi%2FQ%2FWGtiJcU6n4d3%2Bj4yiwcmfOkEs4UcgVfKInHIRcH7TcvgAITk%2F63OtKtwwLxFXytdLonQ2z1yXIvH13qeDtkRaUIT725GGkhFjB6YW7pSz3jPona77NG7bRYGMe32Fw29i7cydaBeyzvb7x2YlaLFwdPm5Z1Ad8CPR1Unckz%2B5CesJ6aojVdM6HAEC4fncnC3AwNOKyDWcNFdnOHxaNtuAykwCOk3pVrOCqi38s06MB4aoyjLOFAoGUSHnbm1%2FI4UN7ABmON4pxww2T%2BSJYxSpe3Mb%2Fw3tWja2BQJ1mprlFwlahNjhMUTtRJNmvMNwmVt739KovaBobpIEvZn1lRFL8ytjW9a71SaPX6GpZEfjkWQRhe20ev4QVo7J7KAI8VZ4DAh2yoWizhqPPqvgAu2xgNDVRZNXwRH0UhxXqOV4y1G%2FY8MFhd1%2Bg2LZNwGpojKWO3FGrBCoNy6tEhadK0rQzxxSSWGmjH7a3ykEwWIarl0ThAI%2BGCIIA4cyTjmFwzkdyXeXzTHMh1CQdpbxzMCyVJ01scDSuuEHzaWItvILZg%2BfjJ03OnJQGh95FZcoJ8Lnd7MD0BuqWbavS0xXAJ3iKtwo9hOaBIgjE6VyKnIcwtGteYlDp%2FDxMuQHIhsMJfKvgIloJszYeHBjrL0agLLY4KcDTCgdfu6fw%2Bf9F5AGxyEbinLeBgJzIlr30Cn3WXDxfaWsFnjD%2F%2FifrjKcGXKUxPDyVEildsCocRayDEUWH1f8N%2BiQ7P0PDDeqcqZ%2BqX2CKyS96pOzOCQK2GITWWSHd9bzGfj9G2VkWhX1AiycgHLwX3RAQ9EijxVc2tYrbSjs4OjBjssdRIb5MnKYs8FG24iTPpKfPMYPgsPAhc%2FyphlDKni8mq9IrQ7uXFgYlJ4hHFmmfcOR%2Fr23mq5A7PdL9ICARFIDkepRii9AGeSx9pkyN0pn0tIIgMk8x4fOG5XjPLuVJcgmbQnXK8Xcy9Tb3YLtkkxHh6eOlLcaibEvtEWKPo8%2B3r7YtaBvVKIGTC8qMKgbCNTRg%2FxAJIAhV%2BzFTxtqWH7e%2Fn2TlAdIvvcTtQ%2FbVX4zlpsNMnzUYM3hgcYCjrirCWcqajrHPd91ZIgeCDQ7SJl3qUovBe0AGpgOQzIYEuEom5b2IJ3WzsqGAZhdxc7phFXL%2FjMYAx%2Bs7QPTJNL5XnSKweYfrL5HIJKjaaPg4iq%2FzeaqiVQNs14Dx5QKEoJ9XxLBAeXVt7aY08duJzZO7LB8UvHwuQXyr2fzpnC%2Fft7WEV5fzm17B%2FGE6yxzOjB7bS7P8H6aNeemjuyWFUm5DIvOpoiwii%2BSeDkhDx99EqGRBocATeEFQXDvKt6gYpjt2t0VKbRmPH4QlXlADgVi3qO5Rpr8UdToMRl0jHUC4P89gzNxiqkNik%2BZvUtoCh8AoP8Gh5Oc6WGYHRW4s2rWrOkIy2Oi1jaR%2BEjIgFDKGATdpUpViqWmQL1BuGG9ua0YrN4dU0kp41Gptxush34zh47Xh4H6YxIJbB%2F3vSrXlnFPcmYoMlVRjlAgi6K0D79LbhwKAV5pGX2EBR%2BAlaMTVMF1bW%2FU00NvIGi63spc3vpcNKwGze11dWF4cZvnylEx%2F2JgwkiqjTm4kSXX5F7finVuHlXjYC0Ui0KU7cvciADhxhpj4TaaT9ROO6ksHZmY%2F%2BB2nJ54dle4GPMTnAZNR72%2FQb7qBfkPFE1KAlKN9JnkcbSvrYPYoLwM%2BthxJwoRN8Qadharw18ENhGDTTM6W6Nq7pGIv7RcxA1nodRlBJ0ONPVnVcVzamr2bh8%2FFEU%2F7v9mlAlNvt62mSYcGkSlOEFKY3lPTtrg1gkk1Cjmwf9RCLb1FP2hy9gMP8B%2FBeMvhi%2FFC1osh0BkjHR3kAwnHo5mJXdJHY9Iw%2FiqgPuknZqj4F%2BQcbPde47EoIlNllnte1lnRyqxvJFFA4Z2LLt35ui6AJDe8gZMeJrKPAM54Kfl8%2BAbvoMybH5qNadQKmxBIz6IhvcjsrrEkwLIcRq%2F%2BKCSC5cA88EekNm%2FCIJeVn9hZfa1aIAdCx1fCrl6r8ZrpWvh6%2FqbeVbKaLdL21GVMiznMWcceUzezCGZuGcUKsrWH3EzEKiGDACDZ5QSFq6yXSYuqotCciHBhXktn5dlLnKP5%2B67vp%2B43WOCpHDmV%2FrAxd0lcd2jTF8gU8YQrAGvNtFcaqhB1A33olq%2BRdYTUlTpLmW5Nq6hj9y%2FwllBvOBuzOEJdsCI6fn8vUiWW74EV9jTUAWp7E3XSpFXCaoA9OEM6h9Oqqa1WkUGneHDkJJvdlvZ%2FXiikTOsLx3Zhzl4wMeF%2FwB2jS%2B8GUtYwDVDn%2BQtlDeuNx9ejw7u%2BPb9OFEuMCiZ2064Vi2SdPceohbZFkkNgh5U0rdTx7bmOL1DJ4SydwBZfJIZSZHjjAjeq9xOcx3qIkRxTfQNNCEdn32DAMiKCs3o6jGknT4ZHDY4sRsGL7r0UwoWJAh34NfwzImisrSLTML3RBPp7CNa98r%2Fum8t8%2FKCGhGM9GC7qZ2XAwGBWm%2BUS8I%2FcwhmUSaTSKJPvDSpdZGMOjhsb3A0QCGOpTGd9jfO9HmaXV9i9nvvVdTqOUbib9YKtHmzb%2BRmR1jJ807qj7je08Dp3R4ngeFtvIbmo3to%2B3T5WN6g2XEcKID1MizjBFFEOuG%2FbZ9uRxqFs0nWdMQe8qzbJIm604zuKIMfc%2Feu0znTbhZmyYwCUeVrFnjTePlUU61a7Zg1JhBNF7rdcJhuZsDiH8C%2BYUkPSm8QSMcjz2paOGnjx9gwdJ0c1P1KCdr5Pj2ujr4V6VmgwbIYIbgudSbmhm0yrLStizG39euvcIPaElX%2Ba1%2BUFF40la8UfrITL8WKxw7qiL0XU%2BKmDcka5NpUIS%2BYrQSv%2BuVE2r8muPu4w2Dsg6kScIma%2FDGT9%2FlJtFY%2BNbw3hC90oNuNCqUaXwKTYoaQ37Yoi2aEcZHBn1WQgLuH6lPMKNBhgLUii3VUs7fXTekzjbaVI%2BgS2%2FfSpS2JA3LU4tQ9jvIH46qtxAlxP1fcLH5rto%2FkMNin%2Bpk6Iq9rkKnuRe0X3M2DWGQQfnBlYxxXC8DTg%2Fkgakr1QelSnVEze%2Bl2HCrEIVBy1kTdLdUOR84kxX3Qg6zTMeQlNPmwy85XQufRRyD5Kh8KxKqPvp%2Bxnt8pI54VY7X%2BXaYtIuhUvkB6i42j5bAADW4cGQFFPrX%2FTJmEHuItEKQ%2FO7g3NhUUjc7giCkeZWRYeFfqrE%2FfNdjHzdo7%2BkmPleMx0DWiHTlOpcLtZMZu0slJta8Y7TtgVQyjCTVvoAmJHGemo1sx5vlSnT7Vv%2F9m0a%2FtUPqFnpHpKc9n81cSHWqjmqnbTBGLF8W4S4slr0ZgYWf2EnGP5eeuHCfY5cgp7E0LvaApF6XSmf37CVRg%2BakteqTwyvYDA9%2BhsqbEVzGUPQLkbmpOBRzvxJN9gd2sVPNjAb3e9IgclVhHfsEq8KNCvFeekTZ%2Fifz3MNHBJYBEKMxIJnveeEZKg%2F%2BoKaUqzNyCMmLZ2SUGbKTkLrqM5wv6%2Bam7f48iAgNYRluhxTcJ86t2FUkBVWwjcQLPmB0nPtSreWxfx8mH4B83ORPlYps%2BHwd0TMoxQfKXP6MDuR2xAvAzptlBkCtVZ%2FNlXDxNCN8VGffzCxAxijXo3VeMOwIp48fQckc%2Bi59wM7qX7ob30p7UBCvTpMUqz1sRB2v1zyJeEytcVZuSm0QIZBji%2B5JZABE%3D,200 18,"-","feeling/223 CFNetwork/808.0.2 Darwin/16.0.0" "-"'''
    # line = '''124.65.163.106,-,[22/Aug/2016:11:30:59 +0800],"POST /appsta.js HTTP/1.1",abc=7sFr5kL0OSImTtj4B%2Fo%2BsQ%3D%3D&params=s4nCmj%2FI3FQWE%2Bj1SahPjy%2B0wMJ1LxBexf1ndF8wVSvD3pQumEWaPAidlSwS+zzIaXF4TF0%2BZ8qyuRxbxkf3%2FlhiSgCnB6cy04IHs48IK9f7uerN0YM9lwddB+Z%2F0OXM55qhMSum4qGeupJzKK6J6NH0a5l6Ox8tq135vGnjjfhFxhw8fxt8cr+0Te5AlTvWzJk7%2F7zn4cWWLOPw1kkzRRBAI%2FDQdDytZe8IJAetYBEp%2F4oZepx+aNdiC39ERkS5LPxJUpxrekKf4OB1qNE0ZHUeKDmjVqfeyMShzkgfNSQ4D5p2+MdxOGgsNcmsuG5v%2FgqljblB5Qqb1Q4eyGBhPagyd%2B5bjjveknKg2En8Hq3hN+QSVdgb0a5iykXJMooKTOjWYR%2F3%2BrB6D4sR8lQZ1UEWvPp5DwOi4tXvFIjQmH+SKr6y2a9moFABwQt8kBlypqYQoNFM4aN18Jy1FPKqjoJznGwx8DIN%2F6vG5QQ+Wd1tZwkxFWc%3D,200 18,"-","Dalvik/2.1.0 (Linux; U; Android 5.1.1; Lenovo K32c36 Build/LMY47V)" "-"'''
    for item in tester_Transform.transform(line):
        print("eeeee", item)