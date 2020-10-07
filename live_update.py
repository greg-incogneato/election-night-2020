
from glob import glob
import pandas as pd
import numpy as np
from datetime import datetime
import xml.etree.ElementTree as ET
import requests 

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

orig_col_list = ['DEM', 'OTHER', 'REP', 'NET_DEM', 'WINNER']

# get florida data
fl_hist = '/Users/greglewis/Desktop/election20/git/election-night-2020/florida_hist.csv'
fl_hist_df = pd.read_csv(fl_hist,  index_col=0)

#update URL on election night
url_FL = "http://fldoselectionfiles.elections.myflorida.com/enightfilespublic/20161108_ElecResultsFL.txt"
election_night_data_FL = read_csv_regex(url_FL, ['ElectionDate'])

# writes to file for archive
# create timestamp and file name
timestamp_for_file = datetime.now().strftime("%d-%b-%Y_%H-%M-%S")
archive_file_name = "/Users/greglewis/Desktop/election20/git/election-night-2020/live_results/FL_election_night_raw_data_extract_%s.xlsx" % timestamp_for_file

# write to excel
election_night_data_FL.to_excel(archive_file_name) 

# get PA data
penn_hist = '/Users/greglewis/Desktop/election20/git/election-night-2020/penn_hist.csv'
penn_hist_df = pd.read_csv(penn_hist,  index_col=0)

# https://www.electionreturns.pa.gov/ElectionFeed/ElectionFeed
url_PA = 'https://electionreturns.pa.gov/electionFeed.aspx?ID=23&FeedName=2020+Primary+Election+by+County'

resp = requests.get(url_PA) 
  
#saving the xml file for parsing
with open('PA_extract.xml', 'wb') as f: 
    f.write(resp.content) 

# create timestamp and file name
timestamp_for_file = datetime.now().strftime("%d-%b-%Y_%H-%M-%S")
archive_file_name = "/Users/greglewis/Desktop/election20/git/election-night-2020/live_results/PA_election_night_raw_data_extract_%s.xml" % timestamp_for_file

# create archive version of xml file
with open(archive_file_name, 'wb') as f: 
    f.write(resp.content) 
    
tree = ET.parse('PA_extract.xml')
root = tree.getroot()

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



def process_live_file(live_df, hist_df, state):
	# process dataframe
	live_df['PartyCode'] = live_df.apply(condense_third_parties, axis=1)
	
	potus_20_raw = pd.pivot_table(live_df[(live_df.RaceCode == 'PRE')], \
	                            index=['CountyName'], columns='PartyCode', values='CanVotes', \
	                            aggfunc=np.sum)


	potus_20_pct = pd.pivot_table(live_df[(live_df.RaceCode == 'PRE')], \
	                            index=['CountyName'], columns='PartyCode', values='CanVotes', \
	                            aggfunc=np.sum).apply(lambda x: x/x.sum(), axis=1).round(4)

	potus_20_raw['NET_DEM'] = potus_20_raw["DEM"] - potus_20_raw["REP"]
	potus_20_raw['WINNER'] = potus_20_raw.apply(county_winner, axis=1)
	
	if state == "PA":
		potus_20_raw['OTHER'] = 0

	potus_20_raw.columns = rename_columns(orig_col_list, '_20_raw')

	potus_20_pct['NET_DEM'] = potus_20_pct["DEM"] - potus_20_pct["REP"]
	potus_20_pct['WINNER'] = potus_20_pct.apply(county_winner, axis=1)
	
	if state == "PA":
		potus_20_pct['OTHER'] = 0

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

# process election night data into full table
full_table_EN_PA = process_live_file(EN_extract_df, penn_hist_df, 'PA')
full_table_EN_FL = process_live_file(election_night_data_FL, fl_hist_df, "FL")
full_table_EN_FL.to_csv("florida_dash.csv", index_label='CountyName') 
full_table_EN_PA.to_csv("penn_dash.csv", index_label='CountyName') 