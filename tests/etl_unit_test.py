import sys,os

sys.path.insert(1,os.getcwd()+ '/ETLProcess')

import json
import unittest
import pandas as pd
import etl_module

class etltestcase(unittest.TestCase):

    def test_importcsv(self):
        readcsv = etl_module.read_input("csv","tests/nyt_test_data.csv")
        self.assertGreaterEqual(readcsv.shape[0],1)

    def test_importurl(self):
        readurl = etl_module.read_input("weburl","https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv")
        self.assertGreaterEqual(readurl.shape[0],1)
    
    @unittest.expectedFailure
    def test_filterdata(self):
        csv = etl_module.read_input("csv","tests/jhk_test_data.csv")
        filtered_data = etl_module.filter_data(csv,'Country/Region','USA')
        self.assertGreaterEqual(filtered_data.shape[0],1)
    
    def test_duplicates(self):
        csv = etl_module.read_input("csv","tests/jhk_test_data.csv")
        csv.name= "duplicatedata"
        duplicates = etl_module.identify_dups(csv,'Date')
        self.assertEqual(duplicates,True)

    def test_columncheck(self):
        csv = etl_module.read_input("csv","tests/jhk_test_data.csv")
        csv.name = "columncheck"
        columns = etl_module.check_columns(csv,['Date','Recovered','Country/Region'])
        self.assertEqual(columns,True)

    @unittest.expectedFailure
    def test_dateconversion(self):
        csv = etl_module.read_input("csv","tests/jhk_test_data.csv")
        df = etl_module.convert_to_datatype(csv,['Date'],"DATE")
        self.assertEqual(df['Date'].dtype,'datetime64[ns]')

    @unittest.expectedFailure
    def test_intconversion(self):
        csv = etl_module.read_input("csv","tests/jhk_test_data.csv")
        df = etl_module.convert_to_datatype(csv,['Recovered'],"INT")
        self.assertEqual(df['Recovered'].dtype,'int64')

    def test_mergedata(self):
        data_1 = etl_module.read_input("csv","tests/jhk_test_data.csv")
        data_2 = etl_module.read_input("csv","tests/nyt_test_data.csv")
        merged_data = etl_module.merge_data(data_1,data_2,'Date','date')
        self.assertEqual(merged_data.shape[0],6)