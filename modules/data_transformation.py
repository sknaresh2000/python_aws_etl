import pandas as pd
from requests import ConnectionError
import requests
import sys
from modules.send_email import send_email

def filter_data(dataf,col_name,value):
    try:
        us_data = dataf[dataf[col_name] == value]
        if us_data.empty:
            send_email(f"No data available after filtering {col_name} by {value}")
            sys.exit(1)
        else:
            return us_data
    except Exception as e:
        send_email(f"Error while filtering data : {e}")
        sys.exit(1)

def identify_dups(dataf,datecol):
    try:
        dup_rows=dataf[dataf.duplicated([datecol],keep=False)]
        if not dup_rows.empty:
            send_email(f"Duplicate date identified in {dataf.name} . Please find the duplicate details \n {dup_rows}")
            sys.exit(1)
    except Exception as e:
            send_email(f"Error while checking duplicate data for {dataf.name}: {e}")
            sys.exit(1)

def check_columns(dataf,col_list):
    for col_name in col_list:
        if not col_name in dataf.columns:
            send_email(f"Required Column Name - {col_name} is not present in {dataf.name}")
            sys.exit(1)

def convert_to_datatype(dataf,col_list,dtype):
    try:
        for col in col_list:
            if dtype == "INT":
                dataf[col] = dataf[col].astype('int64')
            elif dtype == "DATE":
                dataf[col] = pd.to_datetime(dataf[col], format='%Y-%m-%d')
            else:
                send_email(f"Cannot convert {col} to {dtype}. Not a valid input")
        return dataf
    except Exception as e:
        send_email(f"Error while converting {col} to {dtype}: {e}")
        sys.exit(1)

def merge_data(df,df1,dfcol,df1col):
    try:
        merged_data = pd.merge(left=df, right=df1,left_on=dfcol,right_on=df1col)
        if(merged_data.empty):
            send_email("No data available to process after merge")
            sys.exit(1)
        else:
            return merged_data
    except Exception as e:
        send_email(f"Error while merging data : {e}")
        sys.exit(1)