import csv
import sqlite3
import time

import pandas as pd
import pysftp
import json
from datetime import date
# here is the sf connection
import yagmail
from simple_salesforce import Salesforce

 # this is the sandbox
# sf = Salesforce(password='Toronto360', username='eip@accelerize360.com.uat', organizationId='00D6s0000008aQV',
#                 security_token='Xkeuinwc9Rb3xVk67Fb7xTojE', domain='test')
import helper

"""
production
"""
last_file_dir = 'last_aetna.txt'
# sf = Salesforce(password='Toronto360', username='eip@accelerize360.com', organizationId='00D1N000001C94L',
#                 security_token='5XncVr4jQpm87A08izzlgTbmU')

test_path = 'status.csv'


def get_aetna_file_list():
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    sftp = pysftp.Connection(host='eipsftp.medicarefaq.com', port=22, username='Aetna_SFTP',
                             private_key='Aetna_SFTP_Private_Key.pem', cnopts=cnopts)
    with sftp.cd('/eipsftp/Aetna'):
        filtered_iterator = filter(aetna_csv_filter, sftp.listdir())
        filtered_list = []
        for item in filtered_iterator:
            filtered_list.append(item)
        filtered_list.sort(reverse=True)
        return filtered_list


def get_last_file_index():
    # read file

    f = open(last_file_dir, "r")
    last_file = f.read()
    f.close()
    l = get_aetna_file_list()
    for i in range(len(l)):
        if l[i] == last_file:
            return i
    return 0
def send_error_email(df):
    """



    :param df:
    :return:
    """
    df_full_info = pd.read_csv(filepath_or_buffer=test_path, header=0, delimiter=',')

    df_policy = df[['Name']]
    print("mark")
    print(df_full_info)
    df_full_policy = pd.merge(left=df_policy, right=df_full_info, left_on=['Name'], right_on=['POLICY'], how='inner')
    df_full_policy.to_excel("EIP-Aetna-Error.xlsx")
    # receiver = "elite@accelerize360.com"
    receiver = "elite@accelerize360.com"
    body = "Hey All, Please see the attached records which were not updated successfully."
    filename = "EIP-Aetna-Error" +  ".xlsx"

    yag = yagmail.SMTP("yang.wuxuan@accelerize360.com", password="ywx199564")
    yag.send(
        to=receiver,
        subject="EIP-Aetna-Error-" + time.strftime("%Y-%m-%d-%H%M%S", time.localtime()),
        contents=body,
        attachments=filename,
    )

def handle_matched_and_unmatched():
    # get matched data from scenerio 1 and 2
    for i in range(10):
        try:
            df_matched_status, df_matched_csv_policy = match_db_records_by_carrier_and_number()
            if df_matched_status.shape[0] > 0 :

        # update by record id
        # print('updated policy')
        # print(df_matched_status)
                update_status_df(df_matched_status)
                time.sleep(60)
            else:
                # create_case(df_matched_csv_policy)
                break
        except:
            continue
    # create case
    # print(create_case(df_matched_csv_policy))
    df_matched_status, df_matched_csv_policy = match_db_records_by_carrier_and_number()
    df_matched_status_without_pending = helper.filter_pending(df_matched_status)
    if df_matched_status_without_pending.shape[0] > 0:
        send_error_email(df_matched_status_without_pending)



def create_case(df_policy):
    sf = Salesforce(password='Toronto360', username='eip@accelerize360.com', organizationId='00D1N000001C94L',
                    security_token='5XncVr4jQpm87A08izzlgTbmU')
    df = pd.read_csv(filepath_or_buffer=test_path, header=0, delimiter=',')
    df_matched_policy = pd.DataFrame(data=df_policy, columns=["POLICY"])
    df_matched_record = pd.merge(left=df, right=df_matched_policy, on=["POLICY"], how='inner')
    print('dup: ')
    print(df_matched_record)
    df = df.append(df_matched_record)
    df_deduplicate = df.drop_duplicates(keep=False, ignore_index=True)

    description_list = []
    record_dict_list = df_deduplicate.to_dict(orient='records')
    id_result = sf.query("SELECT Id, Name FROM Group WHERE Name = 'Carrier Case Review'")
    id = id_result['records'][0]['Id']
    recordtype_id = get_policy_case_record_type_id_from_salesforce()
    for record in record_dict_list:
        description = json.dumps(record, indent=4)
        description_explanation = 'A Policy was found in ' 'Aetna' + ' database that is not in our system (Salesforce). Please check'+ ' Aetna' + ' portal to confirm this information is accurate and confirm we do not have a Policy record currently in our system. If a Policy does not exist in our system, please create one.'
        new_case = {}
        new_case['OwnerId'] = id
        new_case['Status'] = 'New'
        new_case['Origin'] = 'Carrier Policy Integration'
        new_case['Description'] = description_explanation + ' ' + description
        new_case['Reason'] = 'Policy not found'
        new_case['Subject'] = 'Policy Found w/ Carrier – Not in Salesforce'
        new_case['RecordTypeId'] = recordtype_id

        description_list.append(new_case)
    print(json.dumps(description_list, indent=4))
    print(len(record_dict_list))
    return sf.bulk.Case.insert(description_list, batch_size=100, use_serial=True)


def de_different_withdrawn(x):
    if x['Status__c'] == 'Withdrawn_By_Choosing':
        return 'Withdrawn'
    else:
        return x['Status__c']


def match_db_records_by_carrier_and_number():
    # get the records from sf
    sf_records = get_aetna_records_from_salesforce()
    sf_id_status = pd.DataFrame(sf_records, columns=['Id','Status__c'])

    sf_id_replaced = pd.DataFrame(sf_records, columns=['Id','Status__c'])
    sf_id_replaced = sf_id_replaced[sf_id_replaced['Status__c'] == 'Replaced']

    print(sf_id_replaced)
    print('status in sf: ')
    print(sf_id_status[sf_id_status['Id'] == 'a1n3m000003QwELAA0'])
    sf_id_and_carrier_pnumber = pd.DataFrame(sf_records, columns=['Id', 'Carrier_Policy_Number', 'Unique_Key__c', 'Total_Days_In_Force__c'])
    print(sf_id_status[sf_id_status['Id'] == 'a1n3m000003QwELAA0'])
    # get the db records
    conn = sqlite3.connect('policy.db')
    mycursor = conn.cursor()
    mycursor.execute("select * from aetna_policy ")
    myresult = mycursor.fetchall()
    columns_tuple = mycursor.description
    columns_list = [field_tuple[0] for field_tuple in columns_tuple]
    df = pd.DataFrame(list(myresult), columns=columns_list)
    # df = df.drop(columns='index')
    # print(df)
    # da = df.to_dict(orient='records')
    # print(da)
    df_selected = pd.DataFrame(df, columns=['Status__c', 'Carrier_Policy_Number', 'Unique_Key__c'])
    print('record in db:')
    print(df_selected[df_selected['Carrier_Policy_Number'] == 'Aetna_Testing Aetna 1'])
    # print(df_selected)
    # match, this table contains the rows which both has the same id
    df_match = pd.merge(left=df_selected, right=sf_id_and_carrier_pnumber, on=['Carrier_Policy_Number'], how='inner')
    print('after match')
    print(df_match[df_match['Carrier_Policy_Number'] == 'Aetna_CLI6369988'].to_dict(orient='records'))

    df_matched_policy_number = df_match['Carrier_Policy_Number'].apply(lambda x : x[6 : ])

    df_match = pd.DataFrame(data=df_match, columns=['Id', 'Status__c','Total_Days_In_Force__c', 'Carrier_Policy_Number'])
    print('mark')
    print(df_match['Carrier_Policy_Number'])
    # scenerio 2, matched by the unique key
    df_whole_set_for_s2 = df_selected.copy(deep=True)
    df_s1_records_to_be_excluded = df_match[['Carrier_Policy_Number']]
    df_whole_set_for_s2 = df_whole_set_for_s2.append(df_s1_records_to_be_excluded, ignore_index=True)
    df_whole_set_for_s2 = df_whole_set_for_s2.append(df_s1_records_to_be_excluded, ignore_index=True)
    df_whole_set_for_s2 = df_whole_set_for_s2.drop_duplicates(subset=['Carrier_Policy_Number'], keep=False, ignore_index=True)

    df_unknown = sf_id_and_carrier_pnumber[sf_id_and_carrier_pnumber['Carrier_Policy_Number'] == 'Aetna_Unknown']
    df_matched_unique_key = pd.merge(left=df_whole_set_for_s2, right=df_unknown, on=['Unique_Key__c'], how='inner' )
    df_matched_unique_key_list = df_matched_unique_key['Unique_Key__c']

    # what I want
    df_matched_carrier_x = df_matched_unique_key['Carrier_Policy_Number_x'].apply(lambda x : x[6:])
    df_matched_unique_key = pd.DataFrame(data=df_matched_unique_key, columns=['Id', 'Status__c', 'Total_Days_In_Force__c','Carrier_Policy_Number_x'])

    # all s2
    df_matched_unique_key = df_matched_unique_key.rename(columns={'Carrier_Policy_Number_x' : 'Carrier_Policy_Number'})

    print('df s2: ')
    df_all_s2 = df_matched_unique_key.copy(deep=True)
    df_all_s2 = df_all_s2.rename(columns={'Carrier_Policy_Number' : 'Name'})
    df_all_s2['Name'] = df_all_s2['Name'].apply(lambda x : x[6 : ])
    df_all_s2['Status__c'] = df_all_s2[['Status__c', 'Total_Days_In_Force__c']].apply(lambda x : status_mapping(x), axis=1)
    df_all_s2 = df_all_s2.drop(columns=['Total_Days_In_Force__c'])
    print(df_all_s2)
    # deduplicate scenoerio 1 and 2
    df_match = df_match.append(df_matched_unique_key)
    print("wrong")
    print(df_match[df_match['Id']=="a1n3m000003R1iVAAS"])

    df_sum_up = df_match.drop_duplicates(keep='first', ignore_index=True)
    df_sum_up = df_sum_up.rename(columns={'Carrier_Policy_Number' : 'Name'})
    df_sum_up['Name'] = df_sum_up['Name'].apply(lambda x : x[6 : ])

    # map the status to sf valuesprint('sum up: ')
    #     print(df_sum_upprint('sum up: ')
    # print(df_sum_up)
    df_sum_up['Status__c'] = df_sum_up[['Status__c', 'Total_Days_In_Force__c']].apply(lambda x : status_mapping(x), axis=1)

    df_sum_up = df_sum_up.drop(columns='Total_Days_In_Force__c')
    print('sum up: ')
    print(df_sum_up)

    """
    get the matched policy_Number -> 
    """
    # read csv and get rid of rows with matched policy number and unique_key
    df_sum_carrier_x  = df_matched_policy_number.append(df_matched_carrier_x, ignore_index=True)

    # exclude the matched
    # 1. get the dup:
    # test
    df_sum_up_withdrawn_no_differentiate = pd.DataFrame(data=df_sum_up, columns=['Id', 'Status__c'])
    df_sum_up_withdrawn_no_differentiate['Status__c'] = df_sum_up_withdrawn_no_differentiate[['Status__c','Id']].apply(lambda x : de_different_withdrawn(x), axis=1)
    print('dedifferentiate:')
    print(df_sum_up_withdrawn_no_differentiate)
    """
    left: 0  a1n3m000003QwELAA0  Withdrawn_By_Choosing  
        1  a1n3m000003ANmtAAG             Terminated

    """
    df_status_already_updated = pd.merge(left = df_sum_up_withdrawn_no_differentiate, right= sf_id_status, on=['Id','Status__c'], how='inner')
    print('already matched in sf: ')
    print(df_status_already_updated)

    # 2. dedup
    df_sum_up_with_dup = df_sum_up.append(df_status_already_updated)
    df_sum_up_without_dup = df_sum_up_with_dup.drop_duplicates(subset=['Id'], keep=False,ignore_index=True)


    # all the s2
    df_sum_up_without_dup = df_sum_up_without_dup.append(df_all_s2)
    df_sum_up_without_dup = df_sum_up_without_dup.drop_duplicates(subset=['Id'], keep='first', ignore_index=True)

    # exclude all the replaced
    df_sum_up_without_dup = df_sum_up_without_dup.append(sf_id_replaced)
    df_sum_up_without_dup = df_sum_up_without_dup.append(sf_id_replaced)
    df_sum_up_without_dup = df_sum_up_without_dup.drop_duplicates(subset=['Id'],keep=False,ignore_index=True)

    print('without')
    print(df_sum_up_without_dup)

    return df_sum_up_without_dup, df_sum_carrier_x
    # duplicate check
    # print('duplicate: ')
    # duplicate = df_match[df_match.duplicated('Carrier_Policy_Number')]
    # print(duplicate)


"""
salesforce:
id, policyNUmber, status

csv:
policyNUmbner, status

sf_id, carrierName_policyNumber, sf_status

"""

def get_aetna_csv(i):
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    sftp = pysftp.Connection(host='eipsftp.medicarefaq.com', port=22, username='Aetna_SFTP',
                             private_key='Aetna_SFTP_Private_Key.pem', cnopts=cnopts)
    with sftp.cd('/eipsftp/Aetna'):
        name_list = sftp.listdir()
        print(name_list)
        # read the txt
        sftp.get(remotepath='/eipsftp/Aetna/' + get_csv_name(name_list,i),
                 localpath='./status.txt')
        print(get_csv_name(name_list,i))

    if i == 0:
        f = open(last_file_dir, "w")
        f.write(get_csv_name(name_list,i))
        f.close()

    convert_aetna_txt_to_csv()


"""
To get the sf aetna records with columns:
    --> Salesforce Policy Id
    --> Policy Number
    --> Carrier
    --> Carrier Policy Number
"""

def format_unknown(x):
    if x.capitalize() == 'Unknown':
        return 'Unknown'
    else:
        return x

# def get_aetna_id_and_status_from_salesforce():
#     aetna_set = sf.query_all(
#         "SELECT Id, Name, Status__c, Unique_Key__c, Total_Days_In_Force__c FROM Policy__c where Carrier__r.Parent_Carrier__r.Name = 'Aetna'")
#     df_raw = pd.DataFrame(data=aetna_set['records'])
#     df_selected = pd.DataFrame(df_raw, columns=['Id', 'Status__c'])
#     return df_selected
def change_term_deceased_to_term(x):
    if x == 'Terminated - Deceased':
        return 'Terminated'
    else:
        return x

def get_aetna_records_from_salesforce():
    sf = Salesforce(password='Toronto360', username='eip@accelerize360.com', organizationId='00D1N000001C94L',
                    security_token='5XncVr4jQpm87A08izzlgTbmU')
    aetna_set = sf.query_all(
        "SELECT Id, Name, Unique_Key__c, AWSStatusWithdrawn_Terminated__c, Status__c FROM Policy__c where Carrier__r.Parent_Carrier__r.Name = 'Aetna'")
    df_raw = pd.DataFrame(data=aetna_set['records'])
    df_selected = pd.DataFrame(df_raw, columns=['Id', 'Name', 'Unique_Key__c', 'AWSStatusWithdrawn_Terminated__c','Status__c'])
    df_selected['Name'] = df_selected['Name'].apply(lambda x : format_unknown(x))
    df_selected['Carrier'] = 'Aetna'
    df_selected['Carrier_Policy_Number'] = 'Aetna_' + df_selected['Name'].astype(str)
    df_selected['Id'] = df_selected['Id'].astype(str)
    # df_selected['Status__c'] = df_selected['Status__c'].apply(lambda x :change_term_deceased_to_term(x))
    df_selected = df_selected.rename(columns={'AWSStatusWithdrawn_Terminated__c' : 'Total_Days_In_Force__c'})

    print(df_selected[df_selected['Carrier_Policy_Number'] == 'Aetna_Unknown'])


    return df_selected

def aetna_csv_to_sql(path):
    # read csv from sftp
    # get_humana_csv()

    # select rows
    df = pd.read_csv(filepath_or_buffer=path, header=0, delimiter=',')

    # add a new column
    df['Unique_Key__c'] = df[['INSURED-LAST', 'EFF-DATE', 'ZIP', 'INSURED-BIRTH-DATE', 'PLAN-DESC']].apply(
        lambda x: create_unique_key(x), axis=1)



    print(df['INSURED-LAST'])

    df_selected = pd.DataFrame(df, columns=aetna_get_colums_names())

    # field mapping
    df_selected = df_selected.rename(columns=aetna_get_field_mapping())

    # Add column Carrier
    df_selected['Carrier'] = 'Aetna'

    # Add column Carrier Policy Number
    df_selected['Carrier_Policy_Number'] = df_selected[['Name', 'Carrier']].apply(
        lambda x: x['Carrier'] + '_' + x['Name'], axis=1)

    # Add sf_id column
    df_selected['Id'] = ''

    # save to table
    conn = sqlite3.connect('policy.db')

    print(df_selected)
    df_selected.to_sql(name='aetna_policy', con=conn, if_exists='replace', index=False)


def aetna_get_colums_names():
    return ['POLICY', 'STATUS-REASON', 'Unique_Key__c']


def aetna_get_field_mapping():
    mapping = {'POLICY': 'Name', 'STATUS-REASON': 'Status__c'}
    return mapping


def get_validate_date_format(param):
    if pd.isna(param):
        return str(date.today())
    param = str(param)
    return param[0 : 4] + '-' + param[4 : 6] + '-' + param[6 : 8]


def update_status_df(df):
    """
    read the csv again


    """
    # id, status, policy,
    sf = Salesforce(password='Toronto360', username='eip@accelerize360.com', organizationId='00D1N000001C94L',
                    security_token='5XncVr4jQpm87A08izzlgTbmU')
    df_whole_record = pd.read_csv(filepath_or_buffer=test_path, header=0, delimiter=',')
    df_record_with_date = pd.merge(left=df_whole_record, right=df, how='inner', left_on='POLICY',right_on='Name')
    df_record_with_date['Aetna_Policy_Flag__c'] = True
    df_record_with_date = pd.DataFrame(data=df_record_with_date, columns=['Id','Status__c', 'Name', 'Aetna_Policy_Flag__c', 'ISSUED-DATE', 'TERM-DATE'])
    record_set = df_record_with_date.to_dict(orient='records')
    print('to be updated')
    print(record_set)
    print(len(record_set))

    final_record_set = []

    for record in record_set:
        """
        for all the total_day < 90,
         if it choose withdrawn, -> withdrawn date = termination if not null, if null , set it as today
        """
        if record['Status__c'] == 'Pending Carrier Approval':
            if helper.get_sf_policy_status_bu_id(record['Id'], sf) == 'Pending Carrier Approval':
                final_record_set.append({'Id' : record['Id'], 'Name' : record['Name'], 'Aetna_Policy_Flag__c': True})
                print('will just update the name: ' + str(record))
            else:
                print("won't be updated cuz status_aws->pending, sf_status->not pending: " + str(record))
        # print(record)
        # print('will be updated: ')
        else:
            if record['Status__c'] == 'Active':

                record['Approval_Date__c'] = get_validate_date_format(record['ISSUED-DATE'])
                del record['ISSUED-DATE']
                del record['TERM-DATE']

            elif record['Status__c'] == 'Withdrawn' :
                record['Withdrawn_Date__c'] = str(date.today())
                del record['ISSUED-DATE']
                del record['TERM-DATE']

            elif record['Status__c'] == 'Terminated' or record['Status__c'] == 'Terminated - Deceased':
                record['Termination_Date__c'] = get_validate_date_format(record['TERM-DATE'])
                del record['ISSUED-DATE']
                del record['TERM-DATE']


            else:
                del record['ISSUED-DATE']
                del record['TERM-DATE']

            final_record_set.append(record)

        # remove the replace from the update list

        print(record)

    response_list = sf.bulk.Policy__c.update(final_record_set, batch_size=1, use_serial = True)
    print(response_list)
    # print(response_list)aetna.py:327
    # log_list=  []
    # for response in response_list:
    #     if response['success'] == False:
    #         # need the id
    #         log = {}
    #         log['errors'] = response['errors']
    #         for record in record_set:
    #             if record['Id'] == response['id']:
    #                 log['Id'] = record['Id']
    #                 log['Status__c'] = record['Status__c']
    #                 log['Name'] = record_set['Name']
    #         log_list.append(log)

    # keys = log_list[0].keys()
    # with open('errors.csv', 'w', newline='')  as output_file:
    #     dict_writer = csv.DictWriter(output_file, keys)
    #     dict_writer.writeheader()
    #     dict_writer.writerows(log_list)

def get_csv_name(list,i):
    filtered_iterator = filter(aetna_csv_filter, list)
    filtered_list = []
    for item in filtered_iterator:
        filtered_list.append(item)
    filtered_list.sort(reverse=True)

# {'Id':'', 'Status__c':'Withdrawn', 'Aetna_Policy_Flag__c': True}

    # test
    # print(filtered_list[0])
    # if i == 0:
    #     f = open(last_file_dir, "w")
    #     f.write(filtered_list[i])
    #     f.close()
    return filtered_list[i]


def aetna_csv_filter(filename):
    return filename.endswith('.txt') and 'EliteInsurancePartners' in filename and 'Enrollment_Status' in filename


def convert_aetna_txt_to_csv():
    headers_str = 'COMPANY	POLICY	INSURED-FIRST	INSURED-MIDDLE	INSURED-LAST	ADDRESS-1	ADDRESS-2	CITY	STATE	ZIP	TELEPHONE	HICN	SSN	STATUS	STATUS-REASON	APP-SIG-DATE	EFF-DATE	ISSUED-DATE	TERM-DATE	PLAN-CODE	PLAN-DESC	GROSS-ANN-PREM	MODAL-PREM	BILL-METHOD	BILL-MODE	AGENT-NUMBER	AGENT-FULL-NAME	APPLICATION-ID	INSURED-BIRTH-DATE	UW-CODE	PAID-TO-DATE	ISSUE-STATE	ORIG-ANNUALIZED-PREMIUM	AGENT-SPLIT-PERC	HOUSEHOLD-DISCOUNT 	ISSUE-AGE 	AGENCY-NUMBER 	EMAIL-ADDRESS	GENDER'
    headers_list = headers_str.split()
    df = pd.read_csv('status.txt', sep=',', header=None)
    df = df.rename(columns=get_txt_headers())
    df.to_csv(path_or_buf='status.csv', index=False)
    print(df)


def get_txt_headers():
    headers_str = 'COMPANY	POLICY	INSURED-FIRST	INSURED-MIDDLE	INSURED-LAST	ADDRESS-1	ADDRESS-2	CITY	STATE	ZIP	TELEPHONE	HICN	SSN	STATUS	STATUS-REASON	APP-SIG-DATE	EFF-DATE	ISSUED-DATE	TERM-DATE	PLAN-CODE	PLAN-DESC	GROSS-ANN-PREM	MODAL-PREM	BILL-METHOD	BILL-MODE	AGENT-NUMBER	AGENT-FULL-NAME	APPLICATION-ID	INSURED-BIRTH-DATE	UW-CODE	PAID-TO-DATE	ISSUE-STATE	ORIG-ANNUALIZED-PREMIUM	AGENT-SPLIT-PERC	HOUSEHOLD-DISCOUNT 	ISSUE-AGE 	AGENCY-NUMBER 	EMAIL-ADDRESS	GENDER'
    headers_list = headers_str.split()
    mapping = {}
    for i in range(39):
        mapping[i] = headers_list[i]
    return mapping


def create_unique_key(row):
    # 'INSURED-LAST', 'EFF-DATE', 'ZIP', 'INSURED-BIRTH-DATE', 'PLAN-DESC'
    unique_key = ''

    # deal with last name
    unique_key = unique_key + row['INSURED-LAST'] + '_'

    # deal with DOB
    dob = str(row['INSURED-BIRTH-DATE'])
    year = dob[len(dob) - 4:]
    # print(year)
    day = str(int(dob[len(dob) - 6: len(dob) - 4]))
    # print(day)
    month = str(int(dob[0:len(dob) - 6]))
    # print(month)
    dob_string = year + '_' + month + '_' + day
    unique_key = unique_key + dob_string + '_'

    # deal with zip
    unique_key = unique_key + str(row['ZIP']) + '_'

    # carrier name
    unique_key = unique_key + 'Aetna' + '_'

    # deal with effec date
    effec = str(row['EFF-DATE'])
    effec_year = str(int(effec[0:4]))
    effec_month = str(int(effec[4:6]))
    effec_day = str(int(effec[6:8]))
    effec_date_string = effec_year + '_' + effec_month + '_' + effec_day
    unique_key = unique_key + effec_date_string + '_'

    # deal with coverage
    coverage_map = {
        'Medicare Supplement Plan N': 'Medicare Supplement',
        'Medicare Supplement Plan G': 'Medicare Supplement',
        'ACC Final Exp Level': 'Life (Final Expense)',
        'Cancer and Heart Attack or Stroke': 'Cancer, Heart Attack & Stroke',
        'Cancer Insurance': 'Cancer, Heart Attack & Stroke',
        'Cancer Insurance with Recurrence Benefit': 'Cancer, Heart Attack & Stroke',
        'Cancer Only': 'Cancer, Heart Attack & Stroke',
        'Dental Only 1000': 'DVH',
        'Dental Only 1500': 'DVH',
        'Dental Only 2000': 'DVH',
        'Dental Vision Hearing 1000': 'DVH',
        'Dental Vision Hearing 1500': 'DVH',
        'Dental Vision Hearing 2000': 'DVH',
        'Final Expense Level Benefit': 'Life (Final Expense)',
        'First Diagnosis Cancer Lump Sum': 'Cancer, Heart Attack & Stroke',
        'Heart Attack or Stroke': 'Cancer, Heart Attack & Stroke',
        'Heart Attack or Stroke Insurance': 'Cancer, Heart Attack & Stroke',
        'Heart Attack or Stroke Insurance with Recurrence Benefit': 'Cancer, Heart Attack & Stroke',
        'HIP Flex Daily Hospital Indemnity': 'Hospital Indemnity',
        'HIP Flex Hospital Admission Indemnity': 'Hospital Indemnity',
        'Hospital Indemnity': 'Hospital Indemnity',
        'Med Supp MN Base Copay 2010': 'Medicare Supplement',
        'Med Supp MN Base Hi Ded F 2010': 'Medicare Supplement',
        'Medicare Supplement Base': 'Medicare Supplement',
        'Medicare Supplement Copay Base': 'Medicare Supplement',
        'Medicare Supplement Plan A': 'Medicare Supplement',
        'Medicare Supplement Plan B': 'Medicare Supplement',
        'Medicare Supplement Plan C': 'Medicare Supplement',
        'Medicare Supplement Plan D': 'Medicare Supplement',
        'Medicare Supplement Plan F': 'Medicare Supplement',
        'Medicare Supplement Plan High F': 'Medicare Supplement',
        'Medicare Supplement Plan High G': 'Medicare Supplement',
        'MN Bae & Riders no Rx': 'Medicare Supplement',
        'Recovery Care': 'Medicare Supplement'
    }

    unique_key = unique_key + coverage_map[row['PLAN-DESC']]
    return unique_key

def get_policy_case_record_type_id_from_salesforce():
    sf = Salesforce(password='Toronto360', username='eip@accelerize360.com', organizationId='00D1N000001C94L',
                    security_token='5XncVr4jQpm87A08izzlgTbmU')
    recordtype_id = sf.query("SELECT Id, Name FROM RecordType WHERE Name = 'Policy Case'")
    # print(recordtype_id['records'][0]['Id'])
    return recordtype_id['records'][0]['Id']
def status_mapping(row):
    status_dict = {
        'Active': 'Active',
        'Lapsed No Value': 'IF(Total_Days_In_Force__c < 90, "Withdrawn", "Terminated")',
        'Declined due to Milliman Intel': 'Declined',
        'ESP Underwriting Decline': 'Declined',
        'Cancelled after delivery': 'Withdrawn',
        'App Withdrawn': 'Withdrawn',
        'Approved LB/EFT pending pmt': 'Active',
        'Underwriting Decline': 'Declined',
        'NB/UW Closed Incomplete': 'Withdrawn',
        'Application entered in error': 'Withdrawn',
        'Requested Termination': 'IF(Total_Days_In_Force__c < 90, "Withdrawn", "Terminated")',
        'Medicaid Termination': 'IF(Total_Days_In_Force__c < 90, "Withdrawn", "Terminated")',
        'Terminated - Replaced': 'IF(Total_Days_In_Force__c < 90, "Withdrawn", "Terminated")',
        'Approved DB Pending Pymt': 'Active',
        'Requirements Outstanding': 'Pending Carrier Approval',
        'Pending Underwriting': 'Pending Carrier Approval',
        'your state is not approved': 'Declined',
        'Terminated Due to Death': 'IF(Total_Days_In_Force__c < 90, "Withdrawn", "Terminated")',
        'Terminated Eff Date Reissue': 'IF(Total_Days_In_Force__c < 90, "Withdrawn", "Terminated")',
        'Terminated Unear prm w claims': 'IF(Total_Days_In_Force__c < 90, "Withdrawn", "Terminated")',
        'Reissue': 'Withdrawn',
        'Closed-TI Not Completed': 'Withdrawn',
        'UW Replacement Decline': 'Declined',
        'Decline MedRx': 'Declined',
        'App That was not Taken': 'Withdrawn',
        'Closed-No Valid GI Document': 'Declined',
        'Policy Rescission': 'IF(Total_Days_In_Force__c < 90, "Withdrawn", "Terminated")',
        'Closed-Not Qualified for GI': 'Declined',
        'Benefit Exhausted': 'IF(Total_Days_In_Force__c < 90, "Withdrawn", "Terminated")',
        'UW Deny Pre-screen Questions': 'Declined',
        'Closed-Wrong Application': 'IF(Total_Days_In_Force__c < 90, "Withdrawn", "Terminated")',
        'Approved Pending Payment' : 'Active'
    }

    if row['Status__c'] == 'Terminated Due to Death':
        if row['Total_Days_In_Force__c'] < 90:
            return 'Withdrawn'
        else:
            return 'Terminated - Deceased'
    if status_dict[row['Status__c']] != 'IF(Total_Days_In_Force__c < 90, "Withdrawn", "Terminated")':
        return status_dict[row['Status__c']]
    else:
        if row['Total_Days_In_Force__c'] < 90:
            return 'Withdrawn'
        else:
            return 'Terminated'

#s
# get_policy_case_record_type_id_from_salesforce()
# handle_matched_and_unmatched()
# humana_csv_to_sql(test_path)
def run(i):
    # get the latest file
    get_aetna_csv(i)

    # create db
    aetna_csv_to_sql(test_path)

    # match unmatch
    handle_matched_and_unmatched()

def main_logic():
    # last = get_last_file_index()
    last = get_last_file_index()
    for i in range(last - 1, -1, -1):
        run(i)

# print(get_last_file_index())
# run(0)
# get_humana_csv(1)
# humana_csv_to_sql(test_path)
# match_db_records_by_carrier_and_number()
# run(0)
# get_humana_csv(0)
# humana_csv_to_sql(test_path)
# match_db_records_by_carrier_and_number()
# get_humana_csv(0)
# get_humana_csv(0)
# get_aetna_csv(0)
# aetna_csv_to_sql(test_path)
# match_db_records_by_carrier_and_number()
# handle_matched_and_unmatched()
# handle_matched_and_unmatched()
# get_aetna_csv(0)
# aetna_csv_to_sql(test_path)
# match_db_records_by_carrier_and_number()