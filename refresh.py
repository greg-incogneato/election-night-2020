
import boto3
from datetime import datetime
from urllib.request import urlopen
from io import StringIO
import io
import numpy as np
import pandas as pd
import plotly.express as px
import requests 
import xml.etree.ElementTree as ET
import time

s3 = boto3.resource('s3')

# historical data and other variables
orig_col_list = ['DEM', 'OTHER', 'REP', 'NET_DEM', 'WINNER']

# get florida data
fl_hist = 'https://en2020.s3.amazonaws.com/florida_hist.csv'
fl_hist_df = pd.read_csv(fl_hist,  index_col=0)

# get PA data
penn_hist = 'https://en2020.s3.amazonaws.com/penn_hist.csv'
penn_hist_df = pd.read_csv(penn_hist,  index_col=0)

# update FL url on election night
url_FL_live = "http://fldoselectionfiles.elections.myflorida.com/enightfilespublic/20161108_ElecResultsFL.txt"

url_FL = 'https://en2020.s3.amazonaws.com/FL_data_live.txt'
# https://www.electionreturns.pa.gov/ElectionFeed/ElectionFeed
url_PA_live = 'https://electionreturns.pa.gov/electionFeed.aspx?ID=23&FeedName=2020+Primary+Election+by+County'
url_PA = 'https://en2020.s3.amazonaws.com/PA_data_live.xml'

# function strips white space from downloaded text file
def read_csv_regex(data, date_columns=[]):
    df = pd.read_csv(data, sep="\t", quotechar='"', parse_dates=date_columns)
    
    # remove front and ending blank spaces
    df = df.replace({"^\s*|\s*$":""}, regex=True) 
    
    # if there remained only empty string "", change to Nan
    df = df.replace({"":np.nan}) 
    return df

def county_winner_EN(row):
    if row['Proj_Dem_Vote_Total'] > row['Proj_Rep_Vote_Total']:
        val = 'Biden'
    else:
        val = 'Trump'
    return val

# clean and condense historical vote data
def condense_third_parties(row):
    if row['PartyCode'] == 'REP':
        val = 'REP'
    elif row['PartyCode'] == 'DEM':
        val = 'DEM'
    else:
        val = 'OTHER'
    return val

def county_winner(row):
    if row['DEM'] > row['REP']:
        val = 1
    else:
        val = 2
    return val

def rename_columns(cols, suffix):
    new = []
    for i in range(len(cols)):
        new.append(cols[i] + suffix)
      #  print(cols[i])
    return new

def update_FL(input_url):
    election_night_data_FL = read_csv_regex(input_url, ['ElectionDate'])
    
    # write FL data from election feed to s3
    csv_buffer_FL_live = StringIO()
    election_night_data_FL.to_csv(csv_buffer_FL_live)
    s3.Object('en2020', 'FL_data_live.txt').put(Body=csv_buffer_FL_live.getvalue(), ACL='public-read')

    return election_night_data_FL

def update_PA(input_url):

    resp = requests.get(input_url) 

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


def process_live_file(live_df, hist_df, state):
    # process dataframe
    live_df['PartyCode3'] = live_df.apply(condense_third_parties, axis=1)
    
    potus_20_raw = pd.pivot_table(live_df[(live_df.RaceCode == 'PRE')], \
                                index=['CountyName'], columns='PartyCode3', values='CanVotes', \
                                aggfunc=np.sum)

    potus_20_pct = pd.pivot_table(live_df[(live_df.RaceCode == 'PRE')], \
                                index=['CountyName'], columns='PartyCode3', values='CanVotes', \
                                aggfunc=np.sum).apply(lambda x: x/x.sum(), axis=1).round(4)

    if state == "PA":
        potus_20_raw['OTHER'] = 0
        potus_20_raw = potus_20_raw[['DEM', 'OTHER', 'REP']]

    potus_20_raw['NET_DEM'] = potus_20_raw["DEM"] - potus_20_raw["REP"]
    potus_20_raw['WINNER'] = potus_20_raw.apply(county_winner, axis=1)
    potus_20_raw.columns = rename_columns(orig_col_list, '_20_raw')

    if state == "PA":
        potus_20_pct['OTHER'] = 0
        potus_20_pct = potus_20_pct[['DEM', 'OTHER', 'REP']]

    potus_20_pct['NET_DEM'] = potus_20_pct["DEM"] - potus_20_pct["REP"]
    potus_20_pct['WINNER'] = potus_20_pct.apply(county_winner, axis=1)
    potus_20_pct.columns = rename_columns(orig_col_list, '_20_pct')

    full_table_EN = pd.concat([hist_df,potus_20_raw,potus_20_pct], axis=1)
    full_table_EN['Total_Vote_20'] = full_table_EN['DEM_20_raw'] + full_table_EN['REP_20_raw'] + full_table_EN['OTHER_20_raw']
    full_table_EN['Turnout_20'] = (full_table_EN['Total_Vote_20'] / full_table_EN['Reg_20_Total']).round(3)
    full_table_EN['Expected_20_Vote_Remaining'] = full_table_EN['Expected_2020_Vote'] - full_table_EN['Total_Vote_20']

    full_table_EN['Proj_Dem_Vote_Remaining'] = full_table_EN['DEM_20_pct'] * full_table_EN['Expected_20_Vote_Remaining']
    full_table_EN['Proj_Rep_Vote_Remaining'] = full_table_EN['REP_20_pct'] * full_table_EN['Expected_20_Vote_Remaining']
    full_table_EN['Proj_Other_Vote_Remaining'] = full_table_EN['OTHER_20_pct'] * full_table_EN['Expected_20_Vote_Remaining']

    full_table_EN['Proj_Dem_Vote_Total'] = full_table_EN['DEM_20_raw'] + full_table_EN['Proj_Dem_Vote_Remaining']
    full_table_EN['Proj_Rep_Vote_Total'] = full_table_EN['REP_20_raw'] + full_table_EN['Proj_Rep_Vote_Remaining']
    full_table_EN['Proj_Other_Vote_Total'] = full_table_EN['OTHER_20_raw'] + full_table_EN['Proj_Other_Vote_Remaining']

    full_table_EN['Proj_Dem_Margin'] = (full_table_EN['Proj_Dem_Vote_Total'] - full_table_EN['Proj_Rep_Vote_Total']).round(0)
    full_table_EN['Proj_Winner'] = full_table_EN.apply(county_winner_EN, axis=1)

    return full_table_EN

#############////// live update functions

def refresh_live_data():
    # un-comment these rows to resume live updates from the web!!
    election_night_data_FL = update_FL(url_FL_live)
    election_night_data_PA = update_PA(url_PA_live)
    # process election night data into full table
    full_table_EN_FL = process_live_file(election_night_data_FL, fl_hist_df, "FL")
    full_table_EN_PA = process_live_file(election_night_data_PA, penn_hist_df, 'PA')

    full_table_EN_FL.index.name = 'CountyName'
    csv_buffer_FL = StringIO()
    full_table_EN_FL.to_csv(csv_buffer_FL)
    s3.Object('en2020', 'florida_dash.csv').put(Body=csv_buffer_FL.getvalue(), ACL='public-read')

    full_table_EN_PA.index.name = 'CountyName'
    csv_buffer_PA = StringIO()
    full_table_EN_PA.to_csv(csv_buffer_PA)
    s3.Object('en2020', 'penn_dash.csv').put(Body=csv_buffer_PA.getvalue(), ACL='public-read')
    # end un-comment

refresh_live_data()