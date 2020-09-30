import requests
import pandas as pd
import json, boto3, sys, os, io
from datetime import date, timedelta, datetime
from boto3.dynamodb.conditions import Key, Attr
import modules.data_transformation as dtmodule
from modules.send_email import send_email

os.environ['AWS_PROFILE'] = "AWSCLI"
os.environ['AWS_DEFAULT_REGION'] = "us-east-1"

def read_input(input_type,input):
    try:
        if input_type == "weburl":
            rawdata = requests.get(input)
            return pd.read_csv(io.StringIO(rawdata.content.decode('utf-8')))
        elif input_type == "csv":
            return pd.read_csv(input)
        else:
            send_email("Invalid Input type specified. Unable to retrieve data")
            sys.exit(1)
    except Exception as e:
        send_email(f"Unable to extract information from {input_type} : {e}")
        sys.exit(1)

john_hopkins_url = "https://raw.githubusercontent.com/datasets/covid-19/master/data/time-series-19-covid-combined.csv?opt_id=oeu1600640557504r0.1891205861858345"
nyt_url = "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv"

#US Johns Hopkins dataset
coviddata_all_jh = read_input("weburl",john_hopkins_url)
coviddata_all_jh.name = 'JohnHopkins_Covid_Data'
dtmodule.check_columns(coviddata_all_jh,['Date','Recovered','Country/Region'])

#Filter US Data
coviddata_us_jh = dtmodule.filter_data(coviddata_all_jh,'Country/Region','US')
coviddata_us_jh.name ='JohnHopkins_USCovid_Data'

#NYT dataset - US only
coviddata_us_nyt = read_input("weburl",nyt_url)
coviddata_us_nyt.name ='NYTimes_USCovid_Data'
dtmodule.check_columns(coviddata_us_nyt,["date","cases","deaths"])

#Check for Duplicate data
dtmodule.identify_dups(coviddata_us_jh,'Date')
dtmodule.identify_dups(coviddata_us_nyt,'date')

#Merge Dataset using inner join
merged_covid_data = dtmodule.merge_data(coviddata_us_nyt,coviddata_us_jh,'date','Date')

#Include only the required columns
merged_covid_data = merged_covid_data[["date","Country/Region","cases","deaths","Recovered"]]

#Convert columns to required format
merged_covid_data = dtmodule.convert_to_datatype(merged_covid_data,['cases','deaths','Recovered'],"INT")
merged_covid_data = dtmodule.convert_to_datatype(merged_covid_data,['date'],"DATE")

#Insert a new column for easy retrieval from db
merged_covid_data.insert(loc=0,column='country_reported_month',value=merged_covid_data['date'].dt.strftime("%b-%Y")+':'+merged_covid_data['Country/Region'])

#Get the first and last reported info
last_reported_date = merged_covid_data.tail(1)['date'].values[0]
start_of_month = merged_covid_data.head(1)['country_reported_month'].values[0]

#Initiate connection to dynamodb
session = boto3.session.Session()
dynamodb = session.resource('dynamodb')
covid_data_table = dynamodb.Table('covid_data')

#Check Dynamodb and insert missing data to Dynamodb
i=0
while True:
    #Checks from the current month and goes back till the start of data. Any missing data will be processed
    month_year_filter = (date.today().replace(day=15) - timedelta(days=i)).strftime("%b-%Y")
    try:
        response = covid_data_table.query(
            KeyConditionExpression=Key('country_reported_month').eq(month_year_filter+":US"),
            Limit=1,
            ScanIndexForward=False,
            ReturnConsumedCapacity='TOTAL'
        )
    except Exception as e:
        send_email(f"Connection to dynamodb failed{e}")
        sys.exit(1)
    
    if len(response['Items']):
        last_processed_date = datetime.fromisoformat(response['Items'][0]['date']).strftime("%Y-%m-%d")
        covid_fltr = (merged_covid_data['date'] > last_processed_date) & (merged_covid_data['date'] <= last_reported_date)
        merged_covid_data = merged_covid_data.loc[covid_fltr]
        if merged_covid_data.empty:
            print("No data to process for today.")
            break
        else:  
            push_to_db = True
    else:
        i += 30
        #First Bulk upload if it doesnt find any data.
        if start_of_month == month_year_filter+":US":
            push_to_db = True
        else:
            continue

    if (push_to_db):
        covidlist = merged_covid_data.T.to_dict().values()
        for covidinfo in covidlist:
            covidinfo['date'] = covidinfo['date'].isoformat()
            try:
                response = covid_data_table.put_item(Item=covidinfo,ReturnConsumedCapacity='TOTAL')
            except:
                print(response)
        print("Data has been processed successfully")
        break