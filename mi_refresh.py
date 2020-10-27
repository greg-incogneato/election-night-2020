
import boto3
from dash.dependencies import Input, Output
from datetime import datetime
from glob import glob
from urllib.request import urlopen
import dash
import dash_core_components as dcc
import dash_html_components as html
from io import StringIO
import io
import json
import numpy as np
import pandas as pd
import plotly.express as px
import requests 
import xml.etree.ElementTree as ET
import time
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

s3 = boto3.resource('s3')

url_PA_live = 'https://electionreturns.pa.gov/electionFeed.aspx?ID=23&FeedName=2020+Primary+Election+by+County'
url_PA = 'https://en2020.s3.amazonaws.com/PA_data_live.xml'

def update_PA(input_url):

    resp = requests.get(input_url) 
      
    #saving the xml file for parsing
    # with open('PA_extract.xml', 'wb') as f: 
    #     f.write(resp.content) 

    # create timestamp and file name
    # timestamp_for_file = datetime.now().strftime("%d-%b-%Y_%H-%M-%S")
    # archive_file_name = "live_results/PA_election_night_raw_data_extract_%s.xml" % timestamp_for_file
    # create archive version of xml file
    with open('PA_data_live.xml', 'wb') as f: 
        f.write(resp.content) 
    s3.Bucket('en2020').put_object(Key='PA_data_live.xml', Body=resp.content,  ACL='public-read')
    
    root = ET.fromstring(resp.content)
    # root = tree.getroot()

    # create data frames for extracts from XML feed for PA
    EN_extract_columns = ['RaceCode','CandidateName','PartyCode','CanVotes','PctVote','CountyName']
    EN_extract_df = pd.DataFrame(columns=EN_extract_columns)

    EN_d_reporting_columns = ['CountyName','DistrictsReporting','TotalDistricts','% Reporting']
    EN_d_reporting_df = pd.DataFrame(columns=EN_d_reporting_columns)

    # tables for county data are in root[0][X][1] where x is iterated over (between positions 6 and 72)
    # each table contains two elements: precincts reporting [0] and results [1]
    # put results into a df, put precincts reporting into something else?
    for i in range(6,73):
        county_name = root[0][i][0].text
        
        # extract election results data and write to df
        extract = root[0][i][1].text
        html_df = pd.read_html(extract, header=0)
        county_extract = html_df[1]
        county_extract['CountyName'] = county_name
        county_extract.columns = EN_extract_columns
        EN_extract_df = pd.concat([EN_extract_df,county_extract], axis=0).reset_index(drop=True)
        
        # extract "districts reporting" data and write to df
        districts_reporting_extract = pd.read_html(extract)[0]
        districts_reporting_raw_text = districts_reporting_extract.iloc[0][0]
        districts_reporting_raw_split = districts_reporting_raw_text.split(' ')
        d_reporting = int(districts_reporting_raw_split[0].replace(',', ''))
        d_total = int(districts_reporting_raw_split[3].replace(',', ''))
        d_pct_reporting = d_reporting / d_total

        d_row = pd.DataFrame([[county_name, d_reporting, d_total, d_pct_reporting]], 
                                columns=EN_d_reporting_columns)
        EN_d_reporting_df = pd.concat([EN_d_reporting_df, d_row], axis=0).reset_index(drop=True)

    EN_d_reporting_df.sort_values(by='% Reporting', ascending=False)
    EN_extract_df["RaceCode"].replace({"President of the United States": "PRE"}, inplace=True)
    return EN_extract_df

update_PA(url_PA_live)
