import sqlite3
from datetime import date
import numpy as np
import pandas as pd
import pysftp
from bs4 import BeautifulSoup
from simple_salesforce import Salesforce, format_soql

import time
import yagmail

import helper

file_name = 'moo_status.xml'

last_file_dir = 'last_moo.txt'


def get_sf_connector():
    # sf = Salesforce(password='Toronto360', username='eip@accelerize360.com.uat', organizationId='00D6s0000008aQV',
    #                     security_token='Xkeuinwc9Rb3xVk67Fb7xTojE',domain='test')
    sf = Salesforce(password='Toronto360', username='eip@accelerize360.com', organizationId='00D1N000001C94L',
                    security_token='5XncVr4jQpm87A08izzlgTbmU')
    return sf
def get_last_file_index():
    # read file

    f = open(last_file_dir, "r")
    last_file = f.read()
    f.close()
    l = get_moo_xml_file_list()
    l.sort(reverse=True)
    for i in range(len(l)):
        if l[i] == last_file:
            return i
    return 0


def send_error_email(df):
    # file_name = 'errors' + str(date.today()) + '.xlsx'
    # df.to_excel(file_name)
    # receiver = "gef0604@gmail.com"
    # body = "records which are not updated successfully"
    #
    # yag = yagmail.SMTP("yang.wuxuan@accelerize360.com", password="ywx199564")
    # yag.send(
    #     to=receiver,
    #     subject="errors - MoO-" + str(date.today()),
    #     contents=body,
    #     attachments=file_name,
    # )
    pnum_to_info_dict = get_policy_to_info_dict()
    policy_list = np.array(df['Name']).tolist()
    res = []
    for pnum in policy_list:
        if pnum in pnum_to_info_dict.keys():
            res.append(pnum_to_info_dict[pnum])
        elif 'MOO'+pnum in pnum_to_info_dict.keys():
            res.append(pnum_to_info_dict['MOO'+pnum])
    df_res = pd.DataFrame(res)

    file_name = 'EIP-MoO-Error-2' +  '.xlsx'
    df_res.to_excel(file_name)
    receiver = "elite@accelerize360.com"
    # receiver = "gef0604@gmail.com"
    body = "Hey All, Please see the attached records which were not updated successfully."

    yag = yagmail.SMTP("yang.wuxuan@accelerize360.com", password="ywx199564")
    yag.send(
        to=receiver,
        subject="EIP-MoO-Error-" + time.strftime("%Y-%m-%d-%H%M%S", time.localtime()),
        contents=body,
        attachments=file_name
    )
    # print(get_policy_to_info_dict())


def get_policy_apart_from_aah():
    soup = BeautifulSoup(open(file_name), 'xml')

    policy_db_record_set = []

    policy_xml_set = soup.find_all('Policy')
    for p in policy_xml_set:
        if p.ProductType != None:
            if p.ProductType.string != 'ACCIDENT AND HEALTH':
                policy_db_record_set.append('MoO_' + p.PolNumber.string)
                continue
        else:
            policy_db_record_set.append('MoO_' + p.PolNumber.string)
    df = pd.DataFrame(data=policy_db_record_set, columns=['Name'])
    print('all the policies which is not ACCIDENT AND HEALTH: ')
    print(df)
    return df


def get_required_product_type_and_code_for_scenario2():
    soup = BeautifulSoup(open(file_name), 'xml')

    policy_db_record_set = []

    policy_xml_set = soup.find_all('Policy')
    for p in policy_xml_set:
        if p.ProductType != None:
            if p.ProductType.string == 'ACCIDENT AND HEALTH':
                policy_db_record_set.append('MoO_' + p.PolNumber.string)
                continue
        if p.ProductCode != None:
            if p.ProductCode.string == 'DNTS':
                policy_db_record_set.append('MoO_' + p.PolNumber.string)
    policy_db_record_set.append('MoO_Testing MoO 2')
    df = pd.DataFrame(data=policy_db_record_set, columns=['Name'])
    # print('all the policies with ACCIDENT AND HEALTH and DNTS: ')
    # print(df)
    return df


def status_mapping(row):
    x = row['Status__c']
    x = x.capitalize()
    condition_map = ['Canceled', 'Closed']
    status_map = {
        'Pending': 'Pending Carrier Approval',
        'Issued': 'Active',
        'Declined': 'Declined',
        'PENDING': 'Pending Carrier Approval',
        'Withdrawn': 'Withdrawn',
        'Not taken': 'Withdrawn',
        'Incomplete': 'Withdrawn'
    }

    if x in condition_map:
        if row['AWSStatusWithdrawn_Terminated__c'] < 90:
            return 'Withdrawn'
        else:
            return 'Terminated'
    else:
        return status_map[x]


def handle_matched_and_unmatched():
    # get matched data from scenerio 1 and 2
    for i in range(10):
        try:
            df_matched_status, df_sum_up = match_db_records_by_carrier_and_number()

            print('update size:')
            print(df_matched_status.shape[0])
            if df_matched_status.shape[0] > 0:
                print('The ' + str(i) + ' round: ')
                print(update_status_df(df_matched_status))
                time.sleep(60)
            else:
                # print(create_case(df_sum_up))
                break
        except:
            continue
    df_matched_status, df_sum_up = match_db_records_by_carrier_and_number()
    df_matched_status_without_pending = helper.filter_pending(df_matched_status)
    if df_matched_status_without_pending.shape[0] > 0:
        send_error_email(df_matched_status_without_pending)

    # update by record id


def create_case(df_matched):
    
    sf = get_sf_connector()
    df_policy_list = np.array(df_matched['Name']).tolist()
    print('1 and 2: ')
    print(len(df_policy_list))
    print('matched to list: ')

    """
    step_1 = all_records_xml - scenario1 - scenario2
    
    """
    soup = BeautifulSoup(open(file_name), 'xml')

    policy_xml_set = soup.find_all('Policy')
    dob = soup.find_all('BirthDate')
    lname = soup.find_all('LastName')
    fname = soup.find_all('FirstName')
    id_result = sf.query("SELECT Id, Name FROM Group WHERE Name = 'Carrier Case Review'")
    id = id_result['records'][0]['Id']
    case_list = []
    recordtype_id = get_policy_case_record_type_id_from_salesforce()

    for i in range(len(policy_xml_set)):
        policy = policy_xml_set[i]
        if policy.PolNumber.string not in df_policy_list:
            if if_case_exist(policy.PolNumber.string):
                print('case alrady exist')
                continue
            des = {}
            print('scenario3 criteria 1 passed: ')
            # sf.query_all(
            #     "SELECT Id, Name FROM Policy__c where Name = {}", policy.PolNumber.string)

            res = sf.query_all(
                format_soql("SELECT Id, Name FROM Policy__c where Name = {a}", a=policy.PolNumber.string))
            if res['totalSize'] > 0:
                print('policy exist')
                continue
            without_moo_prefix = policy.PolNumber.string[3:]
            res = sf.query_all(
                format_soql("SELECT Id, Name FROM Policy__c where Name = {a}", a=without_moo_prefix))
            if res['totalSize'] > 0:
                print('policy exist')
                continue

            print('record does not exist in sf, create a case')
            if policy.PolNumber != None:
                des['pnum'] = policy.PolNumber.string
            else:
                des['pnum'] = 'Not specified'
            if policy.PolicyStatus != None:
                des['pstatus'] = policy.PolicyStatus.string
            else:
                des['pstatus'] = 'Not specified'
            if policy.EffDate != None:
                des['peffc'] = policy.EffDate.string
            else:
                des['peffc'] = 'Not specified'
            if policy.Jurisdiction != None:
                des['state'] = policy.Jurisdiction.string
            else:
                des['state'] = 'Not specified'
            if lname[i] != None:
                des['lname'] = lname[i].string
            else:
                des['lname'] = 'Not specified'
            if fname[i] != None:
                des['fname'] = fname[i].string
            else:
                des['fname'] = 'Not specified'
            if dob[i] != None:
                des['dob'] = dob[i].string
            else:
                des['dob'] = 'Not specified'
            if policy.ProductType != None:
                des['ptype'] = policy.ProductType.string
            else:
                des['ptype'] = 'Not specified'

            """
            fname,lname,dob,issueddate
            """
            new_case = {}
            new_case['OwnerId'] = id
            new_case['Status'] = 'New'
            new_case['Origin'] = 'Carrier Policy Integration'
            new_case[
                'Description'] = 'A Policy was found in MoO database that is not in our system (Salesforce). Please check MoO portal to confirm this information is accurate and confirm we do not have a Policy record currently in our system. If a Policy does not exist in our system, please create one. \nDetails:\n'
            new_case['Description'] = new_case['Description'] + 'Policy Number: ' + des['pnum'] + \
                                      '\nPolicy Status: ' + des['pstatus'] + \
                                      '\nEffecive Date: ' + des['peffc'] + \
                                      '\nState: ' + des['state'] + '\nLast Name: ' + des['lname'] + \
                                      '\nFirst Name: ' + des['fname'] + '\nBirth Date: ' + des['dob'] + \
                                      '\nProduct Type: ' + des['ptype']

            new_case['Reason'] = 'Policy not found'
            new_case['RecordTypeId'] = recordtype_id
            new_case['Subject'] = 'Policy Found w/ Carrier – Not in Salesforce'
            print('new cases: ')
            print(policy.PolNumber.string)
            case_list.append(new_case)
            # print(new_case['Description'])

    print(len(case_list))
    print(case_list)
    # description_list = []
    # record_dict_list = df_deduplicate.to_dict(orient='records')
    # id_result = sf.query("SELECT Id, Name FROM Group WHERE Name = 'Carrier Case Review'")
    # id = id_result['records'][0]['Id']
    # for record in record_dict_list:
    #     description = json.dumps(record, indent=4)
    #     new_case = {}
    #     new_case['OwnerId'] = id
    #     new_case['Status'] = 'New'
    #     new_case['Origin'] = 'Carrier Policy Integration'
    #     new_case['Description'] = description
    #     description_list.append(new_case)
    # print(json.dumps(description_list, indent=4))
    return sf.bulk.Case.insert(case_list, batch_size=1, use_serial=True)


def match_db_records_by_carrier_and_number():
    # get the records from sf
    sf_records = get_moo_records_from_salesforce()
    # status of sf
    sf_status = pd.DataFrame(data=sf_records, columns=['Id', 'Status__c'])
    # print('sf status: ')
    # print(sf_status)
    sf_id_and_carrier_pnumber = pd.DataFrame(sf_records, columns=['Id', 'Carrier_Policy_Number', 'UniqueKeyMoO__c',
                                                                  'AWSStatusWithdrawn_Terminated__c'])
    # get the db records
    conn = sqlite3.connect('policy.db')
    mycursor = conn.cursor()
    mycursor.execute("select * from moo_policy ")
    myresult = mycursor.fetchall()
    columns_tuple = mycursor.description
    columns_list = [field_tuple[0] for field_tuple in columns_tuple]
    df = pd.DataFrame(list(myresult), columns=columns_list)
    # df = df.drop(columns='index')
    # print(df)
    # da = df.to_dict(orient='records')
    # print(da)
    df_selected = pd.DataFrame(df, columns=['Status__c', 'Carrier_Policy_Number', 'UniqueKeyMoO__c'])

    # print(df_selected)
    # match, this table contains the rows which both has the same id

    # scenario1,
    df_match = pd.merge(left=df_selected.copy(deep=True), right=sf_id_and_carrier_pnumber, on=['Carrier_Policy_Number'],
                        how='inner')

    df_matched_Id_of_s1 = df_match.copy(deep=True)
    df_matched_Id_of_s1 = df_matched_Id_of_s1[['Carrier_Policy_Number']]
    print('matched number')
    print(df_matched_Id_of_s1)

    df_matched_policy_number = df_match['Carrier_Policy_Number'].apply(lambda x: x[4:])

    df_match = pd.DataFrame(data=df_match,
                            columns=['Id', 'Status__c', 'Carrier_Policy_Number', 'AWSStatusWithdrawn_Terminated__c'])

    print('scenerio1: ')

    # exclude the MOO from db, see if there is anything match
    df_exclude_moo_prefix = df_selected.copy(deep=True)
    # exclude the records matched above - df_match

    df_matched_Id_of_s1 = pd.merge(left=df_exclude_moo_prefix, right=df_matched_Id_of_s1, on=['Carrier_Policy_Number'],
                                   how='inner')
    print('matched in s1 above')
    print(df_matched_Id_of_s1)
    print("before filter: ")
    print(df_exclude_moo_prefix)
    df_exclude_moo_prefix = df_exclude_moo_prefix.append(df_matched_Id_of_s1)
    df_exclude_moo_prefix = df_exclude_moo_prefix.drop_duplicates(subset=['Carrier_Policy_Number'], keep=False,
                                                                  ignore_index=True)
    print("after filter:")
    print(df_exclude_moo_prefix['Carrier_Policy_Number'])
    # MoO_MOO463299741
    # MoO_Testing MoO 1
    df_exclude_moo_prefix['Carrier_Policy_Number'] = df_exclude_moo_prefix['Carrier_Policy_Number'].apply(
        lambda x: 'MoO_' + x[7:])
    df_match_exclude_moo_prefix = pd.merge(left=df_exclude_moo_prefix, right=sf_id_and_carrier_pnumber,
                                           on=['Carrier_Policy_Number'], how='inner')
    print("s1 step 2 match:")
    print(df_match_exclude_moo_prefix[['Carrier_Policy_Number', 'Id']])

    df_match_exclude_moo_prefix = pd.DataFrame(data=df_match_exclude_moo_prefix,
                                               columns=['Id', 'Status__c', 'Carrier_Policy_Number',
                                                        'AWSStatusWithdrawn_Terminated__c'])
    df_match = df_match.append(df_match_exclude_moo_prefix)

    print(df_match)


    # scenerio 2, matched by the unique key
    df_whole_set_for_s2 = df_selected.copy(deep=True)
    df_s1_records_to_be_excluded = df_match[['Carrier_Policy_Number']]
    df_whole_set_for_s2 = df_whole_set_for_s2.append(df_s1_records_to_be_excluded, ignore_index=True)
    df_whole_set_for_s2 = df_whole_set_for_s2.append(df_s1_records_to_be_excluded, ignore_index=True)
    df_whole_set_for_s2 = df_whole_set_for_s2.drop_duplicates(subset=['Carrier_Policy_Number'], keep=False, ignore_index=True)
    print(df_whole_set_for_s2)
    print("mark part")
    print(df_whole_set_for_s2['Carrier_Policy_Number'])
    df_unknown = sf_id_and_carrier_pnumber[sf_id_and_carrier_pnumber['Carrier_Policy_Number'] == 'MoO_Unknown']
    print('get the unknown records from salesforce')
    print(df_unknown)
    # print('mark')
    # print(df_selected['Carrier_Policy_Number'])
    # print('end mark')
    df_matched_unique_key = pd.merge(left=df_whole_set_for_s2, right=df_unknown, on=['UniqueKeyMoO__c'],
                                     how='inner')
    # df_matched_unique_key_list = df_matched_unique_key['Unique_Key__c']

    # what I want
    df_matched_carrier_x = df_matched_unique_key['Carrier_Policy_Number_x'].apply(lambda x: x[4:])
    df_matched_unique_key = pd.DataFrame(data=df_matched_unique_key,
                                         columns=['Id', 'Status__c', 'Carrier_Policy_Number_x',
                                                  'AWSStatusWithdrawn_Terminated__c'])
    print('old scenerio 2(including all the unknown): ')
    df_matched_unique_key = df_matched_unique_key.rename(columns={'Carrier_Policy_Number_x': 'Carrier_Policy_Number'})
    print(df_matched_unique_key)

    # get all the s2
    df_all_s2 = df_matched_unique_key.copy(deep=True)
    df_all_s2 = df_all_s2.rename(columns={'Carrier_Policy_Number': 'Name'})
    df_all_s2['Name'] = df_all_s2['Name'].apply(lambda x: x[4:])
    df_all_s2['Status__c'] = df_all_s2[['Status__c', 'AWSStatusWithdrawn_Terminated__c']].apply(
        lambda x: status_mapping(x), axis=1)
    df_all_s2 = df_all_s2.drop(columns=['AWSStatusWithdrawn_Terminated__c'])
    print('s2: ')
    print(df_all_s2)
    # deduplicate scenoerio 1 and 2
    df_match = df_match.append(df_matched_unique_key)

    df_sum_up = df_match.drop_duplicates(keep='first', ignore_index=True)
    df_sum_up = df_sum_up.rename(columns={'Carrier_Policy_Number': 'Name'})
    df_sum_up['Name'] = df_sum_up['Name'].apply(lambda x: x[4:])
    print('sum up:')
    print(df_sum_up)

    # map the status to sf valuesprint('sum up: ')
    #     print(df_sum_upprint('sum up: ')
    # print(df_sum_up)
    df_sum_up['Status__c'] = df_sum_up[['Status__c', 'AWSStatusWithdrawn_Terminated__c']].apply(
        lambda x: status_mapping(x), axis=1)
    df_sum_up = df_sum_up.drop(columns=['AWSStatusWithdrawn_Terminated__c'])
    print('sum up(including already matched ones):')
    print(df_sum_up)

    """
    get the matched policy_Number -> 
    """
    # read csv and get rid of rows with matched policy number and unique_key
    df_sum_carrier_x = df_matched_policy_number.append(df_matched_carrier_x, ignore_index=True)

    # get the sf record already match before update
    df_already_match = pd.merge(left=df_sum_up, right=sf_status, on=['Id', 'Status__c'], how='inner')
    print('already match before update(will be ignored): ')
    print(df_already_match)

    df_dup = df_sum_up.append(df_already_match, ignore_index=True)
    # get rid of the dup:
    df_after_ignore_matched = df_dup.drop_duplicates(subset=['Id'], keep=False, ignore_index=True)
    print('after ignoring matched: ')
    df_after_ignore_matched = df_after_ignore_matched.append(df_all_s2)
    df_after_ignore_matched = df_after_ignore_matched.drop_duplicates(subset=['Id'], keep='first', ignore_index=True)
    print(df_after_ignore_matched)

    print('x: ')
    print(df_sum_carrier_x)
    return df_after_ignore_matched, df_sum_up


def get_policy_issued_date_dict():
    soup = BeautifulSoup(open(file_name), 'xml')
    policies = soup.find_all('Policy')

    policy_issued_date = {}

    for policy in policies:
        if policy.IssueDate != None:
            policy_issued_date[policy.PolNumber.string] = policy.IssueDate.string
            print(policy.IssueDate.string)
    print(policy_issued_date)
    return policy_issued_date


def update_status_df(df):
    
    sf = get_sf_connector()
    df['Aetna_Policy_Flag__c'] = True
    record_set = df.to_dict(orient='records')
    issued_date = get_policy_issued_date_dict()

    final_record_set = []

    for record in record_set:
        if record['Status__c'] == 'Pending Carrier Approval':
            if helper.get_sf_policy_status_bu_id(record['Id'], sf) == 'Pending Carrier Approval':
                final_record_set.append({'Id' : record['Id'], 'Name' : record['Name'], 'Aetna_Policy_Flag__c': True})
                print('will just update the name: ' + str(record))
        else:
            if record['Status__c'] == 'Active':
                if record['Name'] in issued_date.keys():
                    record['Approval_Date__c'] = issued_date[record['Name']]
                else:
                    record['Approval_Date__c'] = str(date.today())
            elif record['Status__c'] == 'Withdrawn':
                record['Withdrawn_Date__c'] = str(date.today())
            elif record['Status__c'] == 'Terminated':
                record['Termination_Date__c'] = str(date.today())

            final_record_set.append(record)

    print(sf.bulk.Policy__c.update(final_record_set, batch_size=1, use_serial=True))
    return record_set


def format_unknown(x):
    if x.capitalize() == 'Unknown':
        return 'Unknown'
    else:
        return x


"""
:return the dataframe from salesforce including [Id, Name]
"""


def get_moo_records_from_salesforce():
    
    sf = get_sf_connector()
    soup = BeautifulSoup(open(file_name), 'xml')

    policy_db_record_set = []

    policy_xml_set = soup.find_all('Policy')
    print(len(policy_xml_set))
    p_type = soup.find_all('ProductType')
    print(len(p_type))
    p_code = soup.find_all('')

    moo_set = sf.query_all(
        "SELECT Id, Name, Status__c, UniqueKeyMoO__c, AWSStatusWithdrawn_Terminated__c FROM Policy__c where Carrier__r.Parent_Carrier__r.Name = 'Mutual of Omaha'")
    df_raw = pd.DataFrame(data=moo_set['records'])
    df_selected = pd.DataFrame(df_raw, columns=['Id', 'Name', 'UniqueKeyMoO__c', 'Status__c',
                                                'AWSStatusWithdrawn_Terminated__c'])
    df_selected['Carrier'] = 'MoO'
    df_selected['Name'] = df_selected['Name'].apply(lambda x: format_unknown(x))
    df_selected['Carrier_Policy_Number'] = 'MoO_' + df_selected['Name'].astype(str)
    df_selected['UniqueKeyMoO__c'] = df_selected['UniqueKeyMoO__c']
    df_selected = df_selected[df_selected['Status__c'] != 'Replaced']
    print(df_selected['AWSStatusWithdrawn_Terminated__c'])

    # print('s2 record:')
    # record2 =  moo_set = sf.query_all(
    #     "SELECT Id, Name, UniqueKeyMoO__c,Carrier__r.Parent_Carrier__r.Name  FROM Policy__c where id = 'a1n3m000003ANmeAAG'")
    # print(record2)
    print(df_selected)

    return df_selected

    # moo_set = sf.query_all("SELECT Id, Carrier_Name__c,Name FROM Policy__c where Carrier_Name__c like '%Omaha%'")
    # df_raw = pd.DataFrame(data=moo_set['records'])
    # df_selected = pd.DataFrame(df_raw, columns=['Id', 'Name'])
    # df_selected['Carrier'] = 'MoO'
    # df_selected['Carrier_Policy_Number'] = 'MoO_' + df_selected['Name'].astype(str)
    #
    # print(df_selected)
    # return df_selected


"""
:return: the file name for the xml to be downloaded
"""


def get_dir(list_of_xml, i):
    list_of_xml.sort(reverse=True)

    print(list_of_xml[i])
    file = list_of_xml[i]
    # if i == 0:
    #     f = open(last_file_dir, "w")
    #     f.write(file)
    return file


def get_moo_xml(i):
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    sftp = pysftp.Connection(host='eipsftp.medicarefaq.com', port=22, username='MoO_SFTP',
                             private_key='MoO_SFTP_Private_Key.pem', cnopts=cnopts)
    with sftp.cd('/eipsftp/MoO'):
        name_list = sftp.listdir()
        print(name_list)
        sftp.get(remotepath='/eipsftp/MoO/' + get_dir(name_list, i),
                 localpath='./moo_status.xml')

    if i == 0:
        f = open(last_file_dir, "w")
        f.write(get_dir(name_list, i))

def get_moo_xml_file_list():
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    sftp = pysftp.Connection(host='eipsftp.medicarefaq.com', port=22, username='MoO_SFTP',
                             private_key='MoO_SFTP_Private_Key.pem', cnopts=cnopts)
    with sftp.cd('/eipsftp/MoO'):
        return sftp.listdir()


'''
read the status.csv, convert it into dataframe, just 
'''

def map_coverage(s):
    if s['PolicyNumber'].startswith('MOO'):
        return '_Rx (Part D)'
    elif s['ProductType'] == 'ACCIDENT AND HEALTH':
        return '_Medicare Supplement'
    else:
        return ''
    print('coverage')
    print(s)

def moo_xml_to_sql():
    policy_to_info_dict = get_policy_to_info_dict()
    soup = BeautifulSoup(open(file_name), 'xml')

    policy_db_record_set = []

    policy_xml_set = soup.find_all('Policy')

    """
    p1,p2,p3
    n1,n2
    """


    # for i in birth
    """
    [p1,p2,p3,p4....p33]
    
    [i1,,  i2,]
    """
    create_case_list = []
    for i in range(len(policy_xml_set)):
        if 'LastName' not in policy_to_info_dict[policy_xml_set[i].PolNumber.string] or 'BirthDate' not in policy_to_info_dict[policy_xml_set[i].PolNumber.string] or 'EffDate' not in policy_to_info_dict[policy_xml_set[i].PolNumber.string]:
            create_case_list.append(policy_to_info_dict[policy_xml_set[i].PolNumber.string])
            continue
        policy = {}
        policy['Status__c'] = policy_xml_set[i].PolicyStatus.string
        policy['Carrier'] = 'MoO'
        policy['Name'] = policy_xml_set[i].PolNumber.string
        policy['Carrier_Policy_Number'] = policy['Carrier'] + '_' + policy['Name']
        effec = ''
        if policy_xml_set[i].EffDate != None:
            effec = policy_xml_set[i].EffDate.string
            print(policy_to_info_dict[policy_xml_set[i].PolNumber.string])
        coverage = map_coverage(policy_to_info_dict[policy_xml_set[i].PolNumber.string])
        policy['UniqueKeyMoO__c'] = policy_to_info_dict[policy_xml_set[i].PolNumber.string]['LastName'].capitalize() + '_' + validate_format_of_date(policy_to_info_dict[policy_xml_set[i].PolNumber.string]['BirthDate']) + '_' + policy_xml_set[i].Jurisdiction.string + '_' + 'Mutual of Omaha' + '_' + validate_format_of_date(effec) + coverage
        policy['Id'] = ''
        # policy['UniqueKeyMoO__c'] =
        policy_db_record_set.append(policy)
    # iterate all the PolicyUniqueKeyMoO__c

    # print(policy_set)EffDate

    # add test  scenerio 1
    test_1 = {}
    test_1['Status__c'] = 'Closed'
    test_1['Carrier'] = 'MoO'
    test_1['Name'] = 'Testing MoO 1'
    test_1['Carrier_Policy_Number'] = 'MoO' + '_' + test_1['Name']
    test_1['UniqueKeyMoO__c'] = '09-16_01_1956-4-4_FL_Mutual of Omaha_2020-9-9'
    test_1['Id'] = ''
    policy_db_record_set.append(test_1)
    # add test scenerio 2
    test_2 = {}
    test_2['Status__c'] = 'Issued'
    test_2['Carrier'] = 'MoO'
    test_2['Name'] = 'Testing MoO 2'
    test_2['Carrier_Policy_Number'] = 'MoO' + '_' + test_2['Name']
    test_2['UniqueKeyMoO__c'] = '09-16_01_1956-4-4_FL_Mutual of Omaha_2019-12-6'
    test_2['Id'] = ''

    policy_db_record_set.append(test_2)

    df = pd.DataFrame(data=policy_db_record_set)

    conn = sqlite3.connect('policy.db')

    print(df)
    df.to_sql(name='moo_policy', con=conn, if_exists='replace', index=False)

    # create the case for the list

    """
    check if something matches senario1, return the list dont match
    """
    scerario1_not_match = match_scene_1_with_umcompleted_data(create_case_list)
    print(create_case_list)
    # handle_policy_value_missing(create_case_list)
    # print(create_cases_for_value_lost(scerario1_not_match))

def match_scene_1_with_umcompleted_data(create_case_list):
    
    sf = get_sf_connector()

    issued_date = get_policy_issued_date_dict()
    if len(create_case_list) == 0:
        return

    not_match_list = []
    print(create_case_list)
    for i in range(4):
        update_list = []
        for policy in create_case_list:
            query = "SELECT Id, Name, Status__c, AWSStatusWithdrawn_Terminated__c FROM Policy__c where Carrier__r.Parent_Carrier__r.Name = 'Mutual of Omaha' and Name='" + policy['PolicyNumber'] + "' limit 1"
            moo_set = sf.query_all(query)
            print('query res')
            print(moo_set)
            if moo_set['totalSize'] == 0:
                print('here')
                if policy['PolicyNumber'].startswith('MOO'):
                    new_pnum = policy['PolicyNumber'][3:]
                    query = "SELECT Id, Name, Status__c, AWSStatusWithdrawn_Terminated__c FROM Policy__c where Carrier__r.Parent_Carrier__r.Name = 'Mutual of Omaha' and Name='" + new_pnum + "' limit 1"
                    moo_set = sf.query_all(query)

                    if moo_set['totalSize'] == 0:

                        # if policy != None:
                        if i == 0:
                            not_match_list.append(policy)
                    else:

                        record = {}
                        record['Status__c'] = policy['PolicyStatus']
                        record['AWSStatusWithdrawn_Terminated__c'] = moo_set['records'][0]['AWSStatusWithdrawn_Terminated__c']
                        status = status_mapping(record)
                        if status == moo_set['records'][0]['Status__c'] or status == 'Pending Carrier Approval':
                            continue
                        update_body = {}
                        update_body['Id'] = moo_set['records'][0]['Id']
                        update_body['Name'] = moo_set['records'][0]['Name']
                        update_body['Status__c'] = status

                        update_body['Aetna_Policy_Flag__c'] = True
                        if status == 'Active':
                            if update_body['Name'] in issued_date.keys():
                                update_body['Approval_Date__c'] = issued_date[update_body['Name']]
                            else:
                                update_body['Approval_Date__c'] = str(date.today())
                        elif status == 'Withdrawn':
                            update_body['Withdrawn_Date__c'] = str(date.today())
                        elif status == 'Terminated':
                            update_body['Termination_Date__c'] = str(date.today())

                        update_list.append(update_body)
                else:

                    if i == 0:
                        not_match_list.append(policy)

            else:

                record = {}
                record['Status__c'] = policy['PolicyStatus']
                record['AWSStatusWithdrawn_Terminated__c'] = moo_set['records'][0]['AWSStatusWithdrawn_Terminated__c']
                status = status_mapping(record)
                if status == moo_set['records'][0]['Status__c'] or status == 'Pending Carrier Approval':
                    continue
                update_body = {}
                update_body['Id'] = moo_set['records'][0]['Id']
                update_body['Name'] = moo_set['records'][0]['Name']
                update_body['Status__c'] = status

                update_body['Aetna_Policy_Flag__c'] = True
                if status == 'Active':
                    if update_body['Name'] in issued_date.keys():
                        update_body['Approval_Date__c'] = issued_date[update_body['Name']]
                    else:
                        update_body['Approval_Date__c'] = str(date.today())
                elif status == 'Withdrawn':
                    update_body['Withdrawn_Date__c'] = str(date.today())
                elif status == 'Terminated':
                    update_body['Termination_Date__c'] = str(date.today())

                update_list.append(update_body)

        print('match list')
        print(update_list)
        if len(update_list) == 0:
            break
        else:
            if i < 3:
                print(1)
                # print(sf.bulk.Policy__c.update(update_list, batch_size=1, use_serial=True))
            else:
                full_info = get_policy_to_info_dict()
                mail_list = []
                for p in update_list:
                    mail_list.append(full_info[p['Name']])
                df_mail = pd.DataFrame(mail_list)
                file_name = 'EIP-MoO-Error-1.xlsx'
                df_mail.to_excel(file_name)

                # file_name = 'errors-MoO-' + '.xlsx'
                # df_res.to_excel(file_name)
                receiver = "elite@accelerize360.com"
                body = "Hey All, Please see the attached records which were not updated successfully."

                yag = yagmail.SMTP("yang.wuxuan@accelerize360.com", password="ywx199564")
                yag.send(
                    to=receiver,
                    subject="EIP-MoO-Error-" + str(time.strftime("%Y-%m-%d-%H%M%S", time.localtime())),
                    contents=body,
                    attachments=[file_name],
                )

        # print(sf.bulk.Policy__c.update(update_list, batch_size=1, use_serial=True))
        print(1)
    print('not match')
    print(not_match_list)

    return not_match_list

    # for record in record_set:




def handle_policy_value_missing(policy_dict_array):
    print('array')
    print(policy_dict_array)

def create_cases_for_value_lost(create_case_list):
    """
    step_1 = all_records_xml - scenario1 - scenario2

    """
    soup = BeautifulSoup(open(file_name), 'xml')
    
    sf = get_sf_connector()
    # policy_xml_set = soup.find_all('Policy')
    # dob = soup.find_all('BirthDate')
    # lname = soup.find_all('LastName')
    # fname = soup.find_all('FirstName')
    id_result = sf.query("SELECT Id, Name FROM Group WHERE Name = 'Carrier Case Review'")
    id = id_result['records'][0]['Id']
    case_list = []
    recordtype_id = get_policy_case_record_type_id_from_salesforce()

    for policy in create_case_list:
        if True:
        # if not if_case_exist(policy['PolicyNumber']):
            print('case not exist in sf')
            des = {}

            # res = sf.query_all(
            #     format_soql("SELECT Id, Name FROM Policy__c where Name = {a}", a=policy['PolicyNumber']))
            # if res['totalSize'] > 0:
            #     print('policy exist')
            #     continue
            # without_moo_prefix = policy['PolicyNumber'][3:]
            # res = sf.query_all(
            #     format_soql("SELECT Id, Name FROM Policy__c where Name = {a}", a=without_moo_prefix))
            # if res['totalSize'] > 0:
            #     print('policy exist')
            #     continue
            # print('record does not exist in sf, create a case')
            if 'PolicyNumber' in policy.keys():
                des['pnum'] = policy['PolicyNumber']
            else:
                des['pnum'] = 'Not specified'
            if 'PolicyStatus' in policy.keys():
                des['pstatus'] = policy['PolicyStatus']
            else:
                des['pstatus'] = 'Not specified'
            if 'EffDate' in policy.keys():
                des['peffc'] = policy['EffDate']
            else:
                des['peffc'] = 'Not specified'
            if 'Jurisdiction' in policy.keys():
                des['state'] = policy['Jurisdiction']
            else:
                des['state'] = 'Not specified'
            if 'LastName' in policy.keys():
                des['lname'] = policy['LastName']
            else:
                des['lname'] = 'Not specified'
            if 'FirstName' in policy.keys():
                des['fname'] = policy['FirstName']
            else:
                des['fname'] = 'Not specified'
            if 'BirthDate' in policy.keys():
                des['dob'] = policy['BirthDate']
            else:
                des['dob'] = 'Not specified'
            if 'ProductType' in policy.keys():
                des['ptype'] = policy['ProductType']
            else:
                des['ptype'] = 'Not specified'

            """
            fname,lname,dob,issueddate
            """
            new_case = {}
            new_case['OwnerId'] = id
            new_case['Status'] = 'New'
            new_case['Origin'] = 'Carrier Policy Integration'
            new_case[
                'Description'] = 'A Policy was found in MoO database that is not in our system (Salesforce). Please check MoO portal to confirm this information is accurate and confirm we do not have a Policy record currently in our system. If a Policy does not exist in our system, please create one. \nDetails:\n'
            new_case['Description'] = new_case['Description'] + 'Policy Number: ' + des['pnum'] + \
                                      '\nPolicy Status: ' + des['pstatus'] + \
                                      '\nEffecive Date: ' + des['peffc'] + \
                                      '\nState: ' + des['state'] + '\nLast Name: ' + des['lname'] + \
                                      '\nFirst Name: ' + des['fname'] + '\nBirth Date: ' + des['dob'] + \
                                      '\nProduct Type: ' + des['ptype']

            new_case['Reason'] = 'Policy not found'
            new_case['RecordTypeId'] = recordtype_id
            new_case['Subject'] = 'Policy Found w/ Carrier – Not in Salesforce'
            print('new cases: ')
            # print(policy.PolNumber.string)
            case_list.append(new_case)
            # print(new_case['Description'])

    print(len(case_list))
    print(case_list)
    # description_list = []
    # record_dict_list = df_deduplicate.to_dict(orient='records')
    # id_result = sf.query("SELECT Id, Name FROM Group WHERE Name = 'Carrier Case Review'")
    # id = id_result['records'][0]['Id']
    # for record in record_dict_list:
    #     description = json.dumps(record, indent=4)
    #     new_case = {}
    #     new_case['OwnerId'] = id
    #     new_case['Status'] = 'New'
    #     new_case['Origin'] = 'Carrier Policy Integration'
    #     new_case['Description'] = description
    #     description_list.append(new_case)
    # print(json.dumps(description_list, indent=4))
    return sf.bulk.Case.insert(case_list, batch_size=1, use_serial=True)

def validate_format_of_date(date):
    if date == '':
        return ''
    print(date.split('-'))
    year = date.split('-')[0]
    month = str(int(date.split('-')[1]))
    day = str(int(date.split('-')[2]))
    return year + '-' + month + '-' + day


# get_moo_records_from_salesforce()capitalize
# moo_xml_to_sql()

# handle_matched_and_unmatched()
def if_case_exist(policy):
    
    sf = get_sf_connector()
    print('for case:')
    print(policy)
    policy = policy.replace('-', '\-')
    sql = "FIND {" + policy + "} IN ALL FIELDS RETURNING Case(Description)"
    detect = sf.search(sql)
    if len(detect['searchRecords']) == 0:
        return False
    else:
        print(policy + ' already exist')
        return True


def test():
    # search
    # get_moo_xml()
    moo_xml_to_sql()
    # handle_matched_and_unmatched()
    match_db_records_by_carrier_and_number()


def get_policy_case_record_type_id_from_salesforce():
    
    sf = get_sf_connector()
    recordtype_id = sf.query("SELECT Id, Name FROM RecordType WHERE Name = 'Policy Case'")
    # print(recordtype_id['records'][0]['Id'])
    return recordtype_id['records'][0]['Id']


def find_word():
    for i in range(59,100,1):
        get_moo_xml(i)
        f = open("moo_status.xml")
        lines = f.read()
        if "DNTS" in lines:
            print("MOO908411186 in the file: ")
            print("number: " + str(i))


def run(i):
    get_moo_xml(i)
    moo_xml_to_sql()
    handle_matched_and_unmatched()


def main_logic():
    last = get_last_file_index()
    for i in range(last - 1, -1, -1):
        run(i)


def get_policy_to_info_dict():
    res = {}
    soup = BeautifulSoup(open(file_name), 'xml')
    policy_xml_set = soup.find_all('Policy')
    # need pnum -> id
    # need id -> person tag
    # for p in party_set:
    #     print(p['id'])
    party_to_person_dict = create_party_id_to_person_info_dict()
    for pc in policy_xml_set:
        party_id = pc.Life.Coverage.LifeParticipant['PartyID']
        res[pc.PolNumber.string] = party_to_person_dict[party_id]
        res[pc.PolNumber.string]['PolicyNumber'] = pc.PolNumber.string
        if pc.LineOfBusiness != None:
            res[pc.PolNumber.string]['LineOfBusiness'] = pc.LineOfBusiness.string
        if pc.ProductType != None:
            res[pc.PolNumber.string]['ProductType'] = pc.ProductType.string
        if pc.CarrierCode != None:
            res[pc.PolNumber.string]['CarrierCode'] = pc.CarrierCode.string
        if pc.PlanName != None:
            res[pc.PolNumber.string]['PlanName'] = pc.PlanName.string
        if pc.PolicyStatus != None:
            res[pc.PolNumber.string]['PolicyStatus'] = pc.PolicyStatus.string
        if pc.Jurisdiction != None:
            res[pc.PolNumber.string]['Jurisdiction'] = pc.Jurisdiction.string
        if pc.EffDate != None:
            res[pc.PolNumber.string]['EffDate'] = pc.EffDate.string
        if pc.IssueDate != None:
            res[pc.PolNumber.string]['IssueDate'] = pc.IssueDate.string
    return res


def get_person_info_by_party_id(party):
    res = {}
    if party.Person == None:
        return res
    if party.Person.FirstName != None:
        res['FirstName'] = party.Person.FirstName.string
    if party.Person.LastName != None:
        res['LastName'] = party.Person.LastName.string
    if party.Person.BirthDate != None:
        res['BirthDate'] = party.Person.BirthDate.string
    if party.Person.Gender != None:
        res['Gender'] = party.Person.Gender.string
    return res


def create_party_id_to_person_info_dict():
    res = {}
    soup = BeautifulSoup(open(file_name), 'xml')
    party_set = soup.find_all('Party')
    for party in party_set:
        pid = party['id']
        res[pid] = get_person_info_by_party_id(party)
    return res

# run(1)
# get_moo_xml(4)
# moo_xml_to_sql()
# match_db_records_by_carrier_and_number()
# get_moo_xml(0)
# print(get_policy_to_info_dict())
# get_moo_xml(3)
# moo_xml_to_sql()
# print(get_last_file_index())
# print(get_policy_to_info_dict())
# get_moo_xml(0)
# moo_xml_to_sql()
# match_db_records_by_carrier_and_number()
# find_word()
# get_moo_xml(0)
# moo_xml_to_sql()
# match_db_records_by_carrier_and_number()
# a, b = match_db_records_by_carrier_and_number()
