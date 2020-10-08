import json, sys, boto3,os
from boto3.dynamodb.types import TypeDeserializer
from botocore.exceptions import ClientError
from datetime import datetime,timedelta
import pygsheets
import pandas as pd
import time, random

gkey = os.environ["gkey"]
gsheets = os.environ["gsheets"]
from_email = os.environ["from_email"]

def send_email(html_body):
    ses = boto3.client('ses')
    try:
        response = ses.send_email(
            Destination={"ToAddresses":["sknaresh2000@gmail.com"]},
            Message={
                "Body": {
                    "Html": {"Data":html_body}
                },
                "Subject":{"Data":"COVID data for Today"}
            },
            Source=from_email
        )
    except ClientError as e:
        print(e.response["Error"]["Message"])
        return e.response["Error"]["Message"]
    else:
        print("Processed data and Message sent via SES")

def lambda_handler(event,context):
    td = TypeDeserializer()
    html_data = '<td style="border-bottom: 2px solid #cecfd5; padding: 15px;">info</td>'
    html_content = ''
    
    #Get Google API key from SSM Parameter store to store in Googe Sheets
    try:
        ssm = boto3.client('ssm')
        response = ssm.get_parameter(
            Name=gkey,
            WithDecryption=True
        )
        os.environ["GOOGLE_API_CREDENTIALS"]= response['Parameter']['Value']
        gc = pygsheets.authorize(service_account_env_var='GOOGLE_API_CREDENTIALS')
        sh=gc.open(gsheets)
        wk1=sh[0]
    except Exception as e:
        print(f"The lambda function failed while retrieving and setting keys and Google API.{e}")
        return "The lambda function failed while retrieving and setting keys and Google API."

    data =[]
    for record in event['Records']:
        if record['eventName'] == 'INSERT':
            month_info = td.deserialize(record['dynamodb']['NewImage']['country_reported_month'])
            date_info = datetime.fromisoformat(td.deserialize(record['dynamodb']['NewImage']['date'])).strftime("%m-%d-%Y")
            cntry_info = td.deserialize(record['dynamodb']['NewImage']['Country/Region'])
            confirmed_info = str(td.deserialize(record['dynamodb']['NewImage']['cases']))
            recovered_info = str(td.deserialize(record['dynamodb']['NewImage']['Recovered']))
            death_info = str(td.deserialize(record['dynamodb']['NewImage']['deaths']))
            data.append([month_info,date_info,cntry_info,confirmed_info,recovered_info,death_info])       
            
            #HTML design to send an email
            html_content = html_content + "<tr>"
            html_content = html_content + html_data.replace('info',date_info)
            html_content = html_content + html_data.replace('info',cntry_info)
            html_content = html_content + html_data.replace('info',confirmed_info)
            html_content = html_content + html_data.replace('info',recovered_info)
            html_content = html_content + html_data.replace('info',death_info)
            html_content = html_content + "</tr>"
        else:
            print(record['eventName'] + " event was recorded.")
            send_email(record['eventName'] + " event was recorded.Please check CloudWatch logs to know more about this event")
            break
    
    if html_content:
        try:
            df=pd.DataFrame(data,columns=None)
            print(f"Records to process : {df.shape[0]}")

            #To prevent multiple functions updating the Google Sheets at the same time
            if wk1.rows < df.shape[0]:
                num_rows_to_add = df.shape[0] - wks.rows + 1
                wk1.add_rows(num_rows_to_add)
            t = random.randint(0,20)
            if t < df.shape[0]:
                t = df.shape[0] - t
            else:
                t = t - df.shape[0]
            while True:
                time.sleep(t)
                curr_time = datetime.now()
                sh_upd_time = datetime.strptime(sh.updated,'%Y-%m-%dT%H:%M:%S.%fZ') 
                if curr_time - sh_upd_time > timedelta(seconds=10):
                    break

            wk1.set_dataframe(df=df, start=(len(wk1.get_all_records())+2,1), copy_head=False, extend=True)
            wk1.sort_range(start='A2',end='F'+str(wk1.rows),basecolumnindex=0,sortorder='ASCENDING')

            #HTML Email with details
            with open(sys.path[0]+'/email_template.html',"r") as f:
                html_body = f.read()
            html_body = html_body.replace('data',html_content)
            html_body = html_body.replace('tot_number',str(len(event['Records'])))
            send_email(html_body)
        except Exception as e:
            print(e)
            return e