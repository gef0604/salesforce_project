from simple_salesforce import Salesforce

def get_sf_connector():
    # sf = Salesforce(password='Toronto360', username='eip@accelerize360.com.uat', organizationId='00D6s0000008aQV',
    #                     security_token='Xkeuinwc9Rb3xVk67Fb7xTojE',domain='test')
    sf = Salesforce(password='Toronto360', username='eip@accelerize360.com', organizationId='00D1N000001C94L',
                    security_token='5XncVr4jQpm87A08izzlgTbmU')
    return sf

def get_sf_policy_status_bu_id(id, sf):
    query = "select Id, Status__c from Policy__c where Id = 'id'"
    query = query.replace('id', id)
    res = sf.query(query)['records'][0]['Status__c']
    return res

def filter_pending(df):
    return df[df['Status__c'] != 'Pending Carrier Approval']