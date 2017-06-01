# -*- coding: utf-8 -*-
import datetime
import random
from MysqlClient import MysqlClient



def insert_demojr():
    client = MysqlClient()
    con, cur = client.connection
    earliest_date = (datetime.datetime.now() - datetime.timedelta(days=90)).strftime("%Y-%m-%d")
    # today_date = (datetime.datetime.now()).strftime("%Y-%m-%d")
    cur.execute("drop table if exists jhd_demo.jhd_demojr_ios_charge_overall_custom")
    con.commit()
    # cur.execute("create table jhd_demo.jhd_demojr_ios_charge_overall_custom select * from ncf.ncf_360_h5_charge_overall_custom where tm >= '%s'" % earliest_date)
    cur.execute("create table jhd_demo.jhd_demojr_ios_charge_overall_custom select "
                "tm, pub, ver, "
                "uv+FLOOR(uv * RAND()) uv, "
                "reg+FLOOR(reg * RAND()) reg, "
                "charge+FLOOR(charge * RAND()) charge, "
                "today_reg+FLOOR(today_reg * RAND()) today_reg, "
                "today_charge+FLOOR(today_charge * RAND()) today_charge, "
                "first_charge+FLOOR(first_charge * RAND()) first_charge, "
                "reg_charge+FLOOR(reg_charge * RAND()) reg_charge, "
                "recharge+FLOOR(recharge * RAND()) recharge, "
                "charge_7days+FLOOR(charge_7days * RAND()) charge_7days, "
                "checkfin+FLOOR(checkfin * RAND()) checkfin, "
                "auth+FLOOR(auth * RAND()) auth, "
                "cert+FLOOR(cert * RAND()) cert, "
                "bankcard+FLOOR(bankcard * RAND()) bankcard, "
                "charge_newer+FLOOR(charge_newer * RAND()) charge_newer, "
                "dig+FLOOR(dig * RAND()) dig, "
                "dig_uc+FLOOR(dig_uc * RAND()) dig_uc, "
                "dig_homepage+FLOOR(dig_homepage * RAND()) dig_homepage, "
                "dig_jump+FLOOR(dig_jump * RAND()) dig_jump, "
                "dig_cur+FLOOR(dig_cur * RAND()) dig_cur, "
                "dig_curpay+FLOOR(dig_curpay * RAND()) dig_curpay, "
                "uc_invite+FLOOR(uc_invite * RAND()) uc_invite, "
                "check_ucfinc+FLOOR(check_ucfinc * RAND()) check_ucfinc, "
                "paypage+FLOOR(paypage * RAND()) paypage, "
                "charge_success+FLOOR(charge_success * RAND()) charge_success "
                "from ncf.ncf_360_h5_charge_overall_custom where tm >= '%s'" % earliest_date)

    client.closeMysql()

insert_demojr()