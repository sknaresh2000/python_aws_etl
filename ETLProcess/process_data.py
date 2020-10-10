import requests
import pandas as pd
import json, boto3, sys, os, io
from datetime import date, timedelta, datetime
from boto3.dynamodb.conditions import Key, Attr
import etl_module as dtmodule

john_hopkins_url = os.environ['jhk']
nyt_url = os.environ['nyt']
ddb_table = os.environ['ddb_table_name']

def lambda_handler(event,context):
    #US Johns Hopkins dataset
    coviddata_all_jh = dtmodule.read_input("weburl",john_hopkins_url)
    coviddata_all_jh.name = 'JohnHopkins_Covid_Data'
    print("Completed gathering JH data")
    if not dtmodule.check_columns(coviddata_all_jh,['Date','Recovered','Country/Region']):
        return "Required column missing.Please check logs for more details"

    #Filter US Data
    coviddata_us_jh = dtmodule.filter_data(coviddata_all_jh,'Country/Region','US')
    coviddata_us_jh.name ='JohnHopkins_USCovid_Data'

    #NYT dataset - US only
    coviddata_us_nyt = dtmodule.read_input("weburl",nyt_url)
    coviddata_us_nyt.name ='NYTimes_USCovid_Data'
    print("Completed gathering NYT data")
    if not dtmodule.check_columns(coviddata_us_nyt,["date","cases","deaths"]):
        return "Required column missing.Please check logs for more details"

    #Check for Duplicate data
    if dtmodule.identify_dups(coviddata_us_jh,'Date'):
        return "Duplicate data found"
    if dtmodule.identify_dups(coviddata_us_nyt,'date'):
        return "Duplicate data found"

    #Merge Dataset using inner join
    merged_covid_data = dtmodule.merge_data(coviddata_us_nyt,coviddata_us_jh,'date','Date')
    print("Data Merged")

    #Include only the required columns
    merged_covid_data = merged_covid_data[["date","Country/Region","cases","deaths","Recovered"]]
    print("Removed the columns that arent required")

    #Convert columns to required format
    merged_covid_data = dtmodule.convert_to_datatype(merged_covid_data,['cases','deaths','Recovered'],"INT")
    merged_covid_data = dtmodule.convert_to_datatype(merged_covid_data,['date'],"DATE")

    #Insert a new column for easy retrieval from db
    merged_covid_data.insert(loc=0,column='reported_month',value=merged_covid_data['date'].dt.strftime("%b-%Y"))
    print("Inserted Partition key value")

    #Get the first and last reported info
    last_reported_date = merged_covid_data.tail(1)['date'].values[0]
    start_of_month = merged_covid_data.head(1)['reported_month'].values[0]

    #Initiate connection to dynamodb
    session = boto3.session.Session()
    dynamodb = session.resource('dynamodb')
    covid_data_table = dynamodb.Table(ddb_table)

    #Check Dynamodb and insert missing data to Dynamodb
    i=0
    push_to_db = False
    while True:
        #Checks from the current month and goes back till the start of data. Any missing data will be processed
        month_year_filter = (date.today().replace(day=15) - timedelta(days=i)).strftime("%b-%Y")
        try:
            query_response = covid_data_table.query(
                KeyConditionExpression=Key('reported_month').eq(month_year_filter),
                Limit=1,
                ScanIndexForward=False,
                ReturnConsumedCapacity='TOTAL'
            )
            if len(query_response['Items']):
                last_processed_date = datetime.fromisoformat(query_response['Items'][0]['date']).strftime("%Y-%m-%d")
                print(f'Last Processed date{last_processed_date}')
                covid_fltr = (merged_covid_data['date'] > last_processed_date) & (merged_covid_data['date'] <= last_reported_date)
                merged_covid_data = merged_covid_data.loc[covid_fltr]
                if merged_covid_data.empty:
                    print("No data to process for today")
                    return "No data to process for today."
                    break
                else:  
                    push_to_db = True
            else:
                i += 30
                #First Bulk upload if it doesnt find any data.
                if start_of_month == month_year_filter:
                    print("First time upload. Pushing all data")
                    push_to_db = True
                else:
                    continue
        except Exception as e:
            print(f"Error while querying ddb table.{e}")

        if (push_to_db):
            covidlist = merged_covid_data.T.to_dict().values()
            with covid_data_table.batch_writer() as batch:
                for covidinfo in covidlist:
                    covidinfo['date'] = covidinfo['date'].isoformat()
                    try:
                        batch.put_item(Item=covidinfo)
                    except Exception as e:
                        print(f"Error while inserting data to DB. {e}")
            return "Data has been processed for today"
            break