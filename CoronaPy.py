# Import Libs
import pandas as pd
import boto3
import datetime
import time

# Functions
def get_total(df):
    #conf_d_NumOnly = conf_d.drop(['Lat','Long'], axis =1 )
    df['Total_Conf']=df.iloc[:,-1]
    tot_conf = df['Total_Conf'].sum()
    
    return tot_conf

def get_occr_country(df):
    #conf_d_NumOnly = conf_d.drop(['Lat','Long'], axis =1 )
    df['Total_Conf']=df.iloc[:,-1]    
    df = df.groupby(['Country/Region']).sum()['Total_Conf'].sort_values(ascending=False)
    
    return df #returns DF


def get_date(df):
    # Standar Date Format: 2010-11-31
    if list(df.columns)[-1]!='Total_Conf':
        date_txt = list(conf_d.columns)[-1]
    else:
        date_txt = list(conf_d.columns)[-2]
    # print(date_txt)
    date_time = date_txt.split(' ')
    date_2 = date_time[0].split('/')

    if len(date_2[0])==1:
        month_str='0'+date_2[0]
    else:
        month_str=date_2[0]
    if len(date_2[1])==1:
        day_str='0'+date_2[1]
    else:
        day_str=date_2[1]
    if len(date_2[2])==2:
        year_str='20'+date_2[2]
    else:
        year_str=date_2
    
    date_f = '{}-{}-{}'.format(year_str,month_str,day_str)
    return date_f



# Time series CSV Urls ( Updated twice daily )
conf_url =  'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/time_series/time_series_2019-ncov-Confirmed.csv'
death_url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/time_series/time_series_2019-ncov-Deaths.csv'
recov_url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/time_series/time_series_2019-ncov-Recovered.csv'

# Building Dataframes
conf_d  = pd.read_csv(conf_url)
death_d  =pd.read_csv(death_url)
recov_d  =pd.read_csv(recov_url)

# Getting Basic Variables for Dictionary
tot_conf = int(get_total(conf_d))
tot_death = int(get_total(death_d))
tot_recov = int(get_total(recov_d))

occr_conf = get_occr_country(conf_d)
occr_death = get_occr_country(death_d)
occr_recov = get_occr_country(recov_d)

print('Total Confirmed Cases: {}'.format(tot_conf))
print('Total Deaths: {}'.format(tot_death))
print('Total Recovery Cases: {}'.format(tot_recov))
#occr_conf.head()
#occr_death.head()
#occr_recov.head()

a_data = pd.read_csv('accessKeys.csv')
a_key = a_data['Access key ID'][0]
a_S_key = a_data['Secret access key'][0]
region='us-east-2'

# Connecting to DynamoDb
dynamodb = boto3.resource(
    'dynamodb',
    aws_access_key_id=a_key,
    aws_secret_access_key=a_S_key,
    region_name=region
)
dynamoTable = dynamodb.Table('CoronaDB')

types = {'Confirmed':conf_d, 'Deaths':death_d, 'Recoverys':recov_d}

for key in types:
    dic = {}
    tot_c = int(get_total((types[key])))
    date = get_date(types[key])
    dic['Date']=date+' '+key
    dic['Type']=key
    dic['Total Cases']=tot_c
    dic['By Country']=[]
    occr_country = get_occr_country(conf_d)
    for index, value in occr_country.items():
        dic2 = {'Country':index, 'Cases':value}
        dic['By Country'].append(dic2)

    dynamoTable.put_item(Item=dic)
    print('Item for {} uploaded'.format(key))
    time.sleep(5)
