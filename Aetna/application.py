import yagmail
from flask import Flask, request

import aetna
import moo
import humana
import uhc

application = Flask(__name__)

@application.route('/',methods=['GET', 'POST'])
def index():
    # aetna.get_aetna_csv(0)
    # moo.get_moo_xml(0)
    aetna_index = aetna.get_last_file_index()
    moo_index = moo.get_last_file_index()
    humana_index = humana.get_last_file_index()
    uhc_index = uhc.get_last_file_index()
    return 'aetna file index - ' + str(aetna_index) + \
           '\nmoo file index - ' + str(moo_index) + \
           '\nhumana index - ' + str(humana_index) + \
           '\nuhc index - ' + str(uhc_index)

@application.route('/update_uhc',methods=['GET', 'POST'])
def update_uhc():
    flag = request.args.get('flag')
    try:
        uhc.main_logic()
        if flag == '1':
            receiver = "elite@accelerize360.com"
            body = "Successfully updated"

            yag = yagmail.SMTP("yang.wuxuan@accelerize360.com", password="ywx199564")
            yag.send(
                to=receiver,
                subject="EIP-UHC-Success Notice",
                contents=body,
            )
        return 'finish'
    except:
        receiver = "elite@accelerize360.com"
        body = "500 Internal error, please check aws log"

        yag = yagmail.SMTP("yang.wuxuan@accelerize360.com", password="ywx199564")
        yag.send(
            to=receiver,
            subject="EIP-UHC-Server Error",
            contents=body,
        )
        return 'Error email sent'

@application.route('/update_humana',methods=['GET', 'POST'])
def update_humana():
    flag = request.args.get('flag')
    try:
        humana.main_logic()
        if flag == '1':
            receiver = "elite@accelerize360.com"
            body = "Successfully updated"

            yag = yagmail.SMTP("yang.wuxuan@accelerize360.com", password="ywx199564")
            yag.send(
                to=receiver,
                subject="EIP-MoO-Success Notice",
                contents=body,
            )
        return 'finish'
    except:
        receiver = "elite@accelerize360.com"
        body = "500 Internal error, please check aws log"

        yag = yagmail.SMTP("yang.wuxuan@accelerize360.com", password="ywx199564")
        yag.send(
            to=receiver,
            subject="EIP-Humana-Server Error",
            contents=body,
        )
        return 'Error email sent'

@application.route('/update_moo',methods=['GET', 'POST'])
def update_moo():
    flag = request.args.get('flag')
    try:
        moo.main_logic()
        if flag == '1':
            receiver = "elite@accelerize360.com"
            body = "Successfully updated"

            yag = yagmail.SMTP("yang.wuxuan@accelerize360.com", password="ywx199564")
            yag.send(
                to=receiver,
                subject="EIP-MoO-Success Notice",
                contents=body,
            )
        return 'finish'
    except:
        receiver = "elite@accelerize360.com"
        body = "500 Internal error, please check aws log"

        yag = yagmail.SMTP("yang.wuxuan@accelerize360.com", password="ywx199564")
        yag.send(
            to=receiver,
            subject="EIP-MoO-Server Error",
            contents=body,
        )
        return 'Error email sent'
@application.route('/update_aetna',methods=['GET', 'POST'])
def update_aetna():
    try:
        flag = request.args.get('flag')
        aetna.main_logic()
        if flag == '1':
            receiver = "elite@accelerize360.com"
            body = "Successfully updated"

            yag = yagmail.SMTP("yang.wuxuan@accelerize360.com", password="ywx199564")
            yag.send(
                to=receiver,
                subject="EIP-Aetna-Success Notice",
                contents=body,
            )
        return 'finish'
    except:
        receiver = "elite@accelerize360.com"
        body = "500 Internal error, please check aws log"

        yag = yagmail.SMTP("yang.wuxuan@accelerize360.com", password="ywx199564")
        yag.send(
            to=receiver,
            subject="EIP-Aetna-Server Error",
            contents=body,
        )
        return 'Error email sent'
# @application.route('/update',methods=['GET', 'POST'])
# def update():
#     aetna.main_logic()
#     moo.main_logic()


if __name__ == "__main__":
    # Setting debug to True enables debug output. This line should be
    # removed before deploying a production app.
    # application.run()

    application.run()
