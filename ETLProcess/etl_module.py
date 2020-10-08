import pandas as pd
import requests
import io

def read_input(input_type,input):
    try:
        if input_type == "weburl":
            rawdata = requests.get(input)
            return pd.read_csv(io.StringIO(rawdata.content.decode('utf-8')))
        elif input_type == "csv":
            return pd.read_csv(input)
        else:
            print("Invalid input specified")
    except Exception as e:
        print(f"Error while reading data from input{e}")

def filter_data(dataf,col_name,value):
    try:
        us_data = dataf[dataf[col_name] == value]
        if us_data.empty:
            print(f"No data available after filtering {col_name} by {value}")
        else:
            return us_data
    except Exception as e:
        print(f"Error while filtering data : {e}")

def identify_dups(dataf,datecol):
    try:
        dup_rows=dataf[dataf.duplicated([datecol],keep=False)]
        if not dup_rows.empty:
            print(f"Duplicate date identified in {dataf.name} . Please find the duplicate details \n {dup_rows}")
            return True
        return False
    except Exception as e:
            print(f"Error while checking duplicate data for {dataf.name}: {e}")
            
def check_columns(dataf,col_list):
    for col_name in col_list:
        if not col_name in dataf.columns:
            print(f"Required Column Name - {col_name} is not present in {dataf.name}")
            return False
    return True
            
def convert_to_datatype(dataf,col_list,dtype):
    try:
        for col in col_list:
            if dtype == "INT":
                dataf[col] = dataf[col].astype('int64')
            elif dtype == "DATE":
                dataf[col] = pd.to_datetime(dataf[col], format='%Y-%m-%d')
            else:
                print(f"Cannot convert {col} to {dtype}. Not a valid input")
        return dataf
    except Exception as e:
        print(f"Error while converting {col} to {dtype}: {e}")
        
def merge_data(df,df1,dfcol,df1col):
    try:
        merged_data = pd.merge(left=df, right=df1,left_on=dfcol,right_on=df1col)
        if(merged_data.empty):
            print("No data available to process after merge")
        else:
            return merged_data
    except Exception as e:
        print(f"Error while merging data : {e}")