# -*- coding: utf-8 -*-
import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
import email.MIMEBase
import os
import sys
reload(sys)
sys.setdefaultencoding("utf-8")


class EMail(object):

    def send(self, subject, content, recvers=["sudazhuang@jhddg.com"]):
        server = smtplib.SMTP_SSL(host='smtp.exmail.qq.com', port=465)
        server.login('sudazhuang@jhddg.com', '3oxAhCKpbbOTfc2m')
        email = self.create_email(subject, content)
        server.sendmail("sudazhuang@jhddg.com", recvers, email)
        server.quit()

    def create_email(self, subject, content, attachfile=None):
        main_msg = MIMEMultipart()

        if attachfile is None:
            attachfile = []

        file_list = attachfile
        text_msg = MIMEText(content)
        main_msg.attach(text_msg)

        for file_name in file_list:
            contype = 'application/octet-stream'
            maintype, subtype = contype.split('/', 1)

            data = open(file_name, 'rb')
            file_msg = email.MIMEBase.MIMEBase(maintype, subtype)
            file_msg.set_payload(data.read())
            data.close()
            email.Encoders.encode_base64(file_msg)

            basename = os.path.basename(file_name)
            file_msg.add_header('Content-Disposition', 'attachment', filename=basename)
            main_msg.attach(file_msg)

        main_msg['subject'] = subject
        return main_msg.as_string()

if __name__ == "__main__":
    tester = EMail()
    tester.send("test", "66666", ["670108918@qq.com"])