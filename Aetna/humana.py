import csv
import io
import sqlite3
import time
import math
import boto3
import pandas as pd
import pysftp
import json
from datetime import date
# here is the sf connection
import yagmail
from simple_salesforce import Salesforce

import helper

"""
production
"""
last_file_dir = 'last_humana.txt'


test_path = 'humana.csv'

def get_sf_connector():
    # sf = Salesforce(password='Toronto360', username='eip@accelerize360.com.uat', organizationId='00D6s0000008aQV',
    #                     security_token='Xkeuinwc9Rb3xVk67Fb7xTojE',domain='test')
    sf = Salesforce(password='Toronto360', username='eip@accelerize360.com', organizationId='00D1N000001C94L',
                    security_token='5XncVr4jQpm87A08izzlgTbmU')
    return sf

def get_aetna_file_list():
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    sftp = pysftp.Connection(host='eipsftp.medicarefaq.com', port=22, username='Aetna_SFTP',
                             private_key='Aetna_SFTP_Private_Key.pem', cnopts=cnopts)
    with sftp.cd('/eipsftp/Aetna'):
        filtered_iterator = filter(humana_csv_filter, sftp.listdir())
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
    # l = get_aetna_file_list()
    # for i in range(len(l)):
    #     if l[i] == last_file:
    #         return i
    return get_humana_file_index(last_file)
def send_error_email(df):
    """



    :param df:
    :return:
    """
    df_full_info = pd.read_csv(filepath_or_buffer=test_path, header=0, delimiter=',')

    df_policy = df[['Name']]
    print("mark")
    print(df_full_info)
    df_full_policy = pd.merge(left=df_policy, right=df_full_info, left_on=['Name'], right_on=['UMID'], how='inner')
    df_full_policy.to_excel("EIP-Humana-Error.xlsx")
    # receiver = "elite@accelerize360.com"
    receiver = "elite@accelerize360.com"
    body = "Hey All, Please see the attached records which were not updated successfully."
    filename = "EIP-Humana-Error" +  ".xlsx"

    yag = yagmail.SMTP("yang.wuxuan@accelerize360.com", password="ywx199564")
    yag.send(
        to=receiver,
        subject="EIP-Humana-Error-" + time.strftime("%Y-%m-%d-%H%M%S", time.localtime()),
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
    sf = get_sf_connector()
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

def get_count_for_pnum(x, sf):
    # x = {'Id':'a1n3m000003QpOzAAK', 'Carrier_Policy_Number' : 'MoO_H64766215'}
    # sf = get_sf_connector()
    query = """Select Id, Name from Policy__c where Name = 'pnum'"""
    query = query.replace('pnum',x['Carrier_Policy_Number'].split('_')[1])
    res = sf.query(query)['records']
    count = 0
    for item in res:
        if item['Name'] == x['Carrier_Policy_Number'].split('_')[1]:
            count = count + 1
    # print(x['Id'])
    # print(count)
    return count

def match_db_records_by_carrier_and_number():
    # get the records from sf
    sf_records = get_humana_records_from_salesforce()
    sf_id_status = pd.DataFrame(sf_records, columns=['Id','Status__c'])

    sf_id_replaced = pd.DataFrame(sf_records, columns=['Id','Status__c'])
    sf_id_replaced = sf_id_replaced[sf_id_replaced['Status__c'] == 'Replaced']

    sf_id_and_carrier_pnumber = pd.DataFrame(sf_records, columns=['Id', 'Carrier_Policy_Number', 'Unique_Key__c', 'Total_Days_In_Force__c'])
    # get the db records
    conn = sqlite3.connect('policy.db')
    mycursor = conn.cursor()
    mycursor.execute("select * from humana_policy ")
    myresult = mycursor.fetchall()
    columns_tuple = mycursor.description
    columns_list = [field_tuple[0] for field_tuple in columns_tuple]
    df = pd.DataFrame(list(myresult), columns=columns_list)

    df_selected = pd.DataFrame(df, columns=['Status__c', 'Carrier_Policy_Number', 'Unique_Key__c'])

    print(df_selected['Unique_Key__c'])

    df_match = pd.merge(left=df_selected, right=sf_id_and_carrier_pnumber, on=['Carrier_Policy_Number'], how='inner')
    # print('s1')
    # print(df_match['Carrier_Policy_Number'])
    # print('with key')
    # print(df_match['Unique_Key__c'])

    df_matched_policy_number = df_match['Carrier_Policy_Number'].apply(lambda x : x[7 : ])

    """ 
    secario1
    """
    df_match = pd.DataFrame(data=df_match, columns=['Id', 'Status__c','Total_Days_In_Force__c', 'Carrier_Policy_Number'])
    sf = get_sf_connector()
    df_match['Count'] = df_match[['Id', 'Carrier_Policy_Number']].apply(lambda x: get_count_for_pnum(x, sf), axis=1)
    print('df match after first match')
    print(df_match)
    carrier_pnum_greater_than1 = df_match[df_match['Count'] > 1]['Carrier_Policy_Number'].tolist()
    df_carrier_pnum_greater_than1 = df_match[df_match['Count'] > 1]
    df_match = df_match[df_match['Count'] == 1]
    df_match = df_match.drop(columns=['Count'])
    print('s1:')
    print(df_match)
    # print(df_match)
    # print('mark')
    # print(df_match[['Carrier_Policy_Number','Count']])
    # scenerio 2, matched by the unique key
    print('df > 1')
    df_carrier_pnum_greater_than1 = df_carrier_pnum_greater_than1.drop(columns=['Count'])
    df_carrier_pnum_greater_than1 = df_carrier_pnum_greater_than1.drop_duplicates(subset=['Carrier_Policy_Number'])
    df_carrier_pnum_greater_than1 = df_carrier_pnum_greater_than1[['Carrier_Policy_Number']]
    df_carrier_greater_than1 = pd.merge(left=df_carrier_pnum_greater_than1, right=df_selected, on='Carrier_Policy_Number',how='inner')
    # df_carrier_pnum_greater_than1 =
    print(df_carrier_greater_than1)


    # start: remove the records in s1
    df_whole_set_for_s2 = df_selected.copy(deep=True) # Status__c', 'Carrier_Policy_Number', 'Unique_Key__c'
    df_s1_records_to_be_excluded = df_match[['Carrier_Policy_Number']]
    df_whole_set_for_s2 = df_whole_set_for_s2.append(df_s1_records_to_be_excluded, ignore_index=True)
    df_whole_set_for_s2 = df_whole_set_for_s2.append(df_s1_records_to_be_excluded, ignore_index=True)
    df_whole_set_for_s2 = df_whole_set_for_s2.drop_duplicates(subset=['Carrier_Policy_Number'], keep=False, ignore_index=True)
    # end: remove the records in s1

    # start: put the records whose pnum > 1 in scenaio2, df_whole_set_for_s2 below is the whole set for s2
    df_whole_set_for_s2 = df_whole_set_for_s2.append(df_carrier_greater_than1,ignore_index=True)

    print('whole set for s2')
    print(df_whole_set_for_s2[['Carrier_Policy_Number','Unique_Key__c']])
    df_unknown = sf_id_and_carrier_pnumber[sf_id_and_carrier_pnumber['Carrier_Policy_Number'] == 'Humana_Unknown']
    df_count_greater_than1 = sf_id_and_carrier_pnumber[
        sf_id_and_carrier_pnumber.Carrier_Policy_Number.isin(carrier_pnum_greater_than1)]
    df_unknown = df_unknown.append(df_count_greater_than1)
    print(df_unknown['Carrier_Policy_Number'])
    # print('humana unknown')
    # print(df_unknown['Unique_Key__c'])
    # print('sheet s2')
    # print(df_whole_set_for_s2['Unique_Key__c'])
    df_matched_unique_key = pd.merge(left=df_whole_set_for_s2, right=df_unknown, on=['Unique_Key__c'], how='inner' )
    # df_matched_unique_key_list = df_matched_unique_key['Unique_Key__c']
    # print('s2:')
    # print(df_matched_unique_key)
    # what I want
    df_matched_carrier_x = df_matched_unique_key['Carrier_Policy_Number_x'].apply(lambda x : x[7:])
    df_matched_unique_key = pd.DataFrame(data=df_matched_unique_key, columns=['Id', 'Status__c', 'Total_Days_In_Force__c','Carrier_Policy_Number_x'])

    # all s2
    df_matched_unique_key = df_matched_unique_key.rename(columns={'Carrier_Policy_Number_x' : 'Carrier_Policy_Number'})

    # print('df s2: ')
    # print(df_matched_unique_key['Carrier_Policy_Number'])
    df_all_s2 = df_matched_unique_key.copy(deep=True)
    df_all_s2 = df_all_s2.rename(columns={'Carrier_Policy_Number' : 'Name'})
    df_all_s2['Name'] = df_all_s2['Name'].apply(lambda x : x[7 : ])
    df_all_s2['Status__c'] = df_all_s2[['Status__c', 'Total_Days_In_Force__c']].apply(lambda x : status_mapping(x), axis=1)
    df_all_s2 = df_all_s2.drop(columns=['Total_Days_In_Force__c'])
    # print(df_all_s2)
    # deduplicate scenoerio 1 and 2
    df_match = df_match.append(df_matched_unique_key)
    df_sum_up = df_match.drop_duplicates(keep='first', ignore_index=True)
    df_sum_up = df_sum_up.rename(columns={'Carrier_Policy_Number' : 'Name'})
    df_sum_up['Name'] = df_sum_up['Name'].apply(lambda x : x[7 : ])

    # map the status to sf valuesprint('sum up: ')
    #     print(df_sum_upprint('sum up: ')
    # print(df_sum_up)
    df_sum_up['Status__c'] = df_sum_up[['Status__c', 'Total_Days_In_Force__c']].apply(lambda x : status_mapping(x), axis=1)

    df_sum_up = df_sum_up.drop(columns='Total_Days_In_Force__c')
    print('final match')
    print(df_sum_up)
    # print('sum up: ')
    # print(df_sum_up)

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
    # print('dedifferentiate:')
    # print(df_sum_up_withdrawn_no_differentiate)
    """
    left: 0  a1n3m000003QwELAA0  Withdrawn_By_Choosing  
        1  a1n3m000003ANmtAAG             Terminated

    """
    print('sum up')
    print(df_sum_up_withdrawn_no_differentiate)
    df_status_already_updated = pd.merge(left = df_sum_up_withdrawn_no_differentiate, right= sf_id_status, on=['Id','Status__c'], how='inner')
    print('already matched in sf: ')
    print(df_status_already_updated)

    # 2. dedup
    df_sum_up_with_dup = df_sum_up.append(df_status_already_updated)
    df_sum_up_without_dup = df_sum_up_with_dup.drop_duplicates(subset=['Id'], keep=False,ignore_index=True)
    print('without dup 1')
    print(df_sum_up_without_dup)

    # all the s2
    df_sum_up_without_dup = df_sum_up_without_dup.append(df_all_s2)
    df_sum_up_without_dup = df_sum_up_without_dup.drop_duplicates(subset=['Id'], keep='first', ignore_index=True)

    # exclude all the replaced
    df_sum_up_without_dup = df_sum_up_without_dup.append(sf_id_replaced)
    df_sum_up_without_dup = df_sum_up_without_dup.append(sf_id_replaced)
    df_sum_up_without_dup = df_sum_up_without_dup.drop_duplicates(subset=['Id'],keep=False,ignore_index=True)

    # print('without')

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
def get_s3_file_name_list(bucket, dir_name):
    REGION = 'us-east-1'
    ACCESS_KEY_ID = 'AKIASS2EWFEBSAWTVDE4'
    SECRET_ACCESS_KEY = '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'
    conn = boto3.client('s3',
        region_name=REGION,
        aws_access_key_id=ACCESS_KEY_ID,
        aws_secret_access_key=SECRET_ACCESS_KEY)  # again assumes boto.cfg setup, assume AWS S3
    res = []
    for key in conn.list_objects(Bucket=bucket, Prefix=dir_name)['Contents']:
        name = key['Key']
        if name == 'Humana/':
            continue
        res.append(name)
    return res

def read_csv_from_s3(BUCKET_NAME, KEY):
    REGION = 'us-east-1'
    ACCESS_KEY_ID = 'AKIASS2EWFEBSAWTVDE4'
    SECRET_ACCESS_KEY = '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'
    KEY = KEY  # file path in S3
    s3c = boto3.client(
        's3',
        region_name=REGION,
        aws_access_key_id=ACCESS_KEY_ID,
        aws_secret_access_key=SECRET_ACCESS_KEY
    )
    obj = s3c.get_object(Bucket=BUCKET_NAME, Key=KEY)
    return pd.read_csv(io.BytesIO(obj['Body'].read()), encoding='utf8')

def get_humana_file_index(filename):
    bucket = 'eipsftp'
    dir_name = 'Humana'
    name_list = get_s3_file_name_list(bucket, dir_name)
    filtered_iterator = filter(humana_csv_filter, name_list)
    filtered_list = []
    # for item in filtered_iterator:
    #     filtered_list.append(item)
    # filtered_list.sort(reverse=True)
    #
    # print(filtered_list)
    # print(filename)
    for item in filtered_iterator:
        pieces = item.split('_')
        update_month_date_year = pieces[2]
        month = update_month_date_year[0:2]
        day = update_month_date_year[2:4]
        year = update_month_date_year[4:]
        item = item.replace(update_month_date_year, year + month + day)
        filtered_list.append(item)
    filtered_list.sort(reverse=True)
    print(filtered_list)
    # change back
    res = []
    for item in filtered_list:
        print(item)
        pieces = item.split('_')
        update_month_date_year = pieces[2]
        year = update_month_date_year[0:4]
        month = update_month_date_year[4:6]
        day = update_month_date_year[6:]
        item = item.replace(update_month_date_year, month + day + year)
        res.append(item)
    for i in range(len(res)):
        if res[i] == filename:
            return i

    # return 0

def get_humana_csv(i):
    bucket = 'eipsftp'
    dir_name = 'Humana'
    name_list = get_s3_file_name_list(bucket, dir_name)
    file_name = get_csv_name(name_list, i)
    print('file name')
    print(file_name)
    key = file_name
    df_s3 = read_csv_from_s3(bucket, key)
    df_s3.to_csv(test_path,index=False)

    if i == 0:
        f = open(last_file_dir, "w")
        f.write(file_name)
        f.close()

    return df_s3

    """
    1. get the name list, sort and pick the number i
    2. read csv
    3. convert to csv file, same as test path
    """


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
def get_humana_records_from_salesforce():
    # sf = Salesforce(password='Toronto360', username='eip@accelerize360.com.uat', organizationId='00D6s0000008aQV',
    #                 security_token='Xkeuinwc9Rb3xVk67Fb7xTojE', domain='test')
    sf = get_sf_connector()
    aetna_set = sf.query_all(
        "SELECT Id, Name, Unique_Key__c, AWSStatusWithdrawn_Terminated__c, Status__c FROM Policy__c where Carrier__r.Parent_Carrier__r.Name = 'Humana'")
    df_raw = pd.DataFrame(data=aetna_set['records'])
    df_selected = pd.DataFrame(df_raw, columns=['Id', 'Name', 'Unique_Key__c', 'AWSStatusWithdrawn_Terminated__c','Status__c'])
    df_selected['Name'] = df_selected['Name'].apply(lambda x : format_unknown(x))
    df_selected['Carrier'] = 'Humana'
    df_selected['Carrier_Policy_Number'] = 'Humana_' + df_selected['Name'].astype(str)
    df_selected['Id'] = df_selected['Id'].astype(str)
    df_selected['Status__c'] = df_selected['Status__c'].apply(lambda x :change_term_deceased_to_term(x))
    df_selected = df_selected.rename(columns={'AWSStatusWithdrawn_Terminated__c' : 'Total_Days_In_Force__c'})



    return df_selected

def humana_csv_to_sql(path):
    # read csv from sftp
    # get_humana_csv()

    # select rows
    df_full = pd.read_csv(filepath_or_buffer=path, header=0, delimiter=',')

    df_full = df_full[df_full.Product != 'Current MES']

    # get rid of nan
    df_full['DOB'] = df_full['DOB'].astype(str)
    df_full['Zip'] = df_full['Zip'].astype(str)
    df_full = df_full[df_full['DOB'] != 'nan']

    # add a new column
    df_full['Unique_Key__c'] = df_full[['Member', 'Eff_Date', 'Zip', 'DOB', 'Product']].apply(
        lambda x: create_unique_key(x), axis=1)

    df_full['UMID'] = df_full['UMID'].astype(str)


    df_selected = pd.DataFrame(df_full, columns=aetna_get_colums_names())

    # field mapping
    df_selected = df_selected.rename(columns=aetna_get_field_mapping())

    # Add column Carrier
    df_selected['Carrier'] = 'Humana'

    # Add column Carrier Policy Number
    df_selected['Carrier_Policy_Number'] = df_selected[['Name', 'Carrier']].apply(
        lambda x: x['Carrier'] + '_' + x['Name'], axis=1)

    # Add sf_id column
    df_selected['Id'] = ''

    # save to table
    conn = sqlite3.connect('policy.db')

    print(df_selected)
    df_selected.to_sql(name='humana_policy', con=conn, if_exists='replace', index=False)


def aetna_get_colums_names():
    return ['UMID', 'Term_Date', 'Unique_Key__c']


def aetna_get_field_mapping():
    mapping = {'UMID': 'Name', 'Term_Date': 'Status__c'}
    return mapping

def isNaN(string):
    return string != string

def get_validate_date_format(param):
    if pd.isna(param):
        return str(date.today())
    param = str(param)
    ter_date = param.split('/')
    return ter_date[2] + '-' + ter_date[0] + '-' + ter_date[1]


def update_status_df(df):
    """
    read the csv again


    """
    # id, status, policy,
    sf = get_sf_connector()

    df_whole_record = pd.read_csv(filepath_or_buffer=test_path, header=0, delimiter=',')
    df_record_with_date = pd.merge(left=df_whole_record, right=df, how='inner', left_on='UMID',right_on='Name')
    df_record_with_date['Aetna_Policy_Flag__c'] = True
    df_record_with_date = pd.DataFrame(data=df_record_with_date, columns=['Id','Status__c', 'Name', 'Aetna_Policy_Flag__c','Term_Date'])
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

        # print(record)
        # print('will be updated: ')
        if record['Status__c'] == 'Pending Carrier Approval':
            if helper.get_sf_policy_status_bu_id(record['Id'], sf) == 'Pending Carrier Approval':
                final_record_set.append({'Id' : record['Id'], 'Name' : record['Name'], 'Aetna_Policy_Flag__c': True})
                print('will just update the name: ' + str(record))
            else:
                print("won't be updated cuz status_aws->pending, sf_status->not pending: " + str(record))

        else:
            if record['Status__c'] == 'Active':

                record['Approval_Date__c'] = str(date.today())
                del record['Term_Date']

            elif record['Status__c'] == 'Withdrawn' :
                if 'Term_Date' not in record.keys() or record['Term_Date'] == None:
                    record['Withdrawn_Date__c'] = str(date.today())
                else:
                    record['Withdrawn_Date__c'] = get_validate_date_format(record['Term_Date'])
                # del record['ISSUED-DATE']
                del record['Term_Date']

            elif record['Status__c'] == 'Terminated':
                record['Termination_Date__c'] = get_validate_date_format(record['Term_Date'])
                # del record['ISSUED-DATE']
                # if 'Term_Date' not in record.keys() or record['Term_Date'] == None:
                #     record['Termination_Date__c'] = str(date.today())
                # else:
                #     record['Termination_Date__c'] = get_validate_date_format(record['Term_Date'])
                del record['Term_Date']


            else:
                # del record['ISSUED-DATE']
                del record['Term_Date']

            final_record_set.append(record)

        # remove the replace from the update list

        # print(record)
    print(final_record_set)
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
    filtered_iterator = filter(humana_csv_filter, list)
    filtered_list = []


    # 2, sort
    for item in filtered_iterator:
        pieces = item.split('_')
        update_month_date_year = pieces[2]
        month = update_month_date_year[0:2]
        day = update_month_date_year[2:4]
        year = update_month_date_year[4:]
        item = item.replace(update_month_date_year, year + month + day)
        filtered_list.append(item)
    filtered_list.sort(reverse=True)
    print(filtered_list)
    # change back
    res = []
    for item in filtered_list:
        print(item)
        pieces = item.split('_')
        update_month_date_year = pieces[2]
        year = update_month_date_year[0:4]
        month = update_month_date_year[4:6]
        day = update_month_date_year[6:]
        item = item.replace(update_month_date_year, month + day + year)
        res.append(item)
        # print(item)
    # 3, change it back
    # {'Id':'', 'Status__c':'Withdrawn', 'Aetna_Policy_Flag__c': True}

    # test
    # print(filtered_list[0])
    # if i == 0:
    #     f = open(last_file_dir, "w")
    #     f.write(filtered_list[i])
    #     f.close()
    # print(res)
    print(res[i])
    return res[i]


def humana_csv_filter(filename):
    # return filename.endswith('.txt') and 'EliteInsurancePartners' in filename and 'Enrollment_Status' in filename
    return 'EP10' in filename

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
    # row['Member'] = str(row['Member'])
    full_name = row['Member'].replace(' ','')
    last_name = full_name.split(',')[0]
    last_name = last_name.capitalize()
    unique_key = unique_key + last_name + '_'

    # deal with DOB


    dob = str(row['DOB']).split('/')
    # print(dob)
    year = dob[2]
    # print(year)
    day = dob[1]
    # print(day)
    month = dob[0]
    # print(month)
    dob_string = year + '_' + month + '_' + day
    unique_key = unique_key + dob_string + '_'

    # deal with zip
    index_of_dot = row['Zip'].index('.')
    unique_key = unique_key + str(row['Zip'][0:index_of_dot]) + '_'

    # carrier name
    unique_key = unique_key + 'Humana' + '_'

    # deal with effec date
    effec = str(row['Eff_Date']).split('/')
    effec_year = effec[2]
    effec_month = effec[0]
    effec_day = effec[1]
    effec_date_string = effec_year + '_' + effec_month + '_' + effec_day
    unique_key = unique_key + effec_date_string + '_'

    coverage = row['Product'].replace(' ','') if 'HMO' in row['Product'] else row['Product']
    # deal with coverage
    coverage_map = {
        'PDP':'Rx (Part D)',
        'Current MES' : 'Medicare Supplement',
        'HMO' : 'Medicare Advantage',
        'PPO' : 'Medicare Advantage',
        'CP-HMO' : 'Medicare Advantage'
    }

    unique_key = unique_key + coverage_map[coverage]
    return unique_key

def get_policy_case_record_type_id_from_salesforce():

    sf = get_sf_connector()
    recordtype_id = sf.query("SELECT Id, Name FROM RecordType WHERE Name = 'Policy Case'")
    # print(recordtype_id['records'][0]['Id'])
    return recordtype_id['records'][0]['Id']
def status_mapping(row):
    if row['Status__c'] == '12/31/9999':
        return 'Active'
    elif row['Total_Days_In_Force__c'] < 90:
            return 'Withdrawn'
    else:
            return 'Terminated'

#s
# get_policy_case_record_type_id_from_salesforce()
# handle_matched_and_unmatched()
# humana_csv_to_sql(test_path)
def run(i):
    # get the latest file
    get_humana_csv(i)

    # create db
    humana_csv_to_sql(test_path)

    # match unmatch
    handle_matched_and_unmatched()

def main_logic():
    # last = get_last_file_index()
    last = get_last_file_index()
    print(last)
    for i in range(last - 1, -1, -1):
        run(i)

get_humana_csv(0)
# run(0)
humana_csv_to_sql(test_path)
match_db_records_by_carrier_and_number()
# handle_matched_and_unmatched()
# handle_matched_and_unmatched()
# match_db_records_by_carrier_and_number()
# get_last_file_index()
# humana_csv_to_sql(test_path)
# get_humana_csv(0)
# run(0)
# match_db_records_by_carrier_and_number()
# humana_csv_to_sql(test_path)
# match_db_records_by_carrier_and_number()
# get_humana_csv(0)
# sf = get_sf_connector()
# res= sf.bulk.Policy__c.update([{'Id': 'a1n3m000003R1t4AAC', 'Status__c': 'Terminated', 'Name': 'H73144386', 'Aetna_Policy_Flag__c': True, 'Termination_Date__c': '2020-12-31'}, {'Id': 'a1n3m000003R4DxAAK', 'Status__c': 'Terminated', 'Name': 'H74787927', 'Aetna_Policy_Flag__c': True, 'Termination_Date__c': '2020-12-31'}, {'Id': 'a1n3m000003R3ZgAAK', 'Status__c': 'Terminated', 'Name': 'H71117882', 'Aetna_Policy_Flag__c': True, 'Termination_Date__c': '2020-12-31'}, {'Id': 'a1n3m000003R0w7AAC', 'Status__c': 'Terminated', 'Name': 'H62614504', 'Aetna_Policy_Flag__c': True, 'Termination_Date__c': '2020-12-31'}, {'Id': 'a1n3m000003R2KUAA0', 'Status__c': 'Terminated', 'Name': 'H62623561', 'Aetna_Policy_Flag__c': True, 'Termination_Date__c': '2020-12-31'}, {'Id': 'a1n3m000003Qw7eAAC', 'Status__c': 'Terminated', 'Name': 'H64804822', 'Aetna_Policy_Flag__c': True, 'Termination_Date__c': '2021-02-28'}, {'Id': 'a1n3m000003R3tdAAC', 'Status__c': 'Terminated', 'Name': 'H73160893', 'Aetna_Policy_Flag__c': True, 'Termination_Date__c': '2020-12-31'}, {'Id': 'a1n3m000003R2uqAAC', 'Status__c': 'Terminated', 'Name': 'H77130923', 'Aetna_Policy_Flag__c': True, 'Termination_Date__c': '2020-12-31'}, {'Id': 'a1n3m000003R1tOAAS', 'Status__c': 'Terminated', 'Name': 'H62621138', 'Aetna_Policy_Flag__c': True, 'Termination_Date__c': '2020-12-31'}, {'Id': 'a1n3m000003QyMvAAK', 'Status__c': 'Terminated', 'Name': 'H67013629', 'Aetna_Policy_Flag__c': True, 'Termination_Date__c': '2020-12-31'}, {'Id': 'a1n3m000003R4t0AAC', 'Status__c': 'Terminated', 'Name': 'H64880109', 'Aetna_Policy_Flag__c': True, 'Termination_Date__c': '2020-12-31'}, {'Id': 'a1n3m000003QxkIAAS', 'Status__c': 'Terminated', 'Name': 'H75133726', 'Aetna_Policy_Flag__c': True, 'Termination_Date__c': '2020-12-31'}]
# , batch_size=20, use_serial = True)
# print(res)
# run(0)