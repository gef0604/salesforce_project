import time
from datetime import date
from uhc import convert_dat_to_csv
import boto3
import pandas as pd
import io
from humana import get_sf_connector
# REGION = 'us-east-1'
# ACCESS_KEY_ID = 'AKIASS2EWFEBSAWTVDE4'
# SECRET_ACCESS_KEY = '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'
# BUCKET_NAME = 'eipsftp'
# KEY = 'UHC/Status_elite_20200923.dat' # file path in S3
# s3c = boto3.client(
#         's3',
#         region_name = REGION,
#         aws_access_key_id = ACCESS_KEY_ID,
#         aws_secret_access_key = SECRET_ACCESS_KEY
#     )
# obj = s3c.get_object(Bucket= BUCKET_NAME , Key = KEY)
# df = pd.read_csv(io.BytesIO(obj['Body'].read()), encoding='utf8')
# print(df)
# s3c.download_file(BUCKET_NAME, KEY, 'uhc.dat')
# convert_dat_to_csv('uhc.dat','uhc.csv')

# s = 'D1YV8E18MV61 A03082468           MS GROSS                              JOANN                                             T        10272020102720201101202012312020          1025195486030614109 NEWBERRY ROAD                                                                                               EAST HADDAM                   CT06423G01VOLUNTARY                                         CURRENTLY CARRIES OTHER COVERAGE                  20301WB927522                                                                                                                                                                                                                                                                                                                               1YV8E18MV61 Y                                                             0               3037909451     22456727  '
# print(len('UnitedHealthcare_'))

# for i in range(3):
#
#
#     for j in range(7):
#         if j == 4:
#             continue
#         print(j)
b = ''
obj = 'Contact'
global query

a = """
sf = get_sf_connector()
b = sf.bulk.Account.query(query)
"""
a = a.replace('Account', obj)
exec(a,{'query':'SELECT Id, Name FROM Account', 'get_sf_connector' : get_sf_connector})
print(b)