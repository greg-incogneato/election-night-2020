# import modules

from glob import glob
import pandas as pd
import numpy as np

# read historical vote data
filenames = glob('/Users/greglewis/Desktop/election20/git/election-night-2020/historical_data/past_results/pa*.xlsx')
dataframes = [pd.read_excel(f) for f in filenames]
concatenated_df = pd.concat(dataframes, ignore_index=True)

region_file = '/Users/greglewis/Desktop/election20/git/election-night-2020/historical_data/pa_regions.xlsx'
region_df = pd.read_excel(region_file, index_col=0)

# load voter registration data
reg_2016_file = '/Users/greglewis/Desktop/election20/git/election-night-2020/historical_data/registration/2020gen_party_pa.xlsx'
reg_2020_file = '/Users/greglewis/Desktop/election20/git/election-night-2020/historical_data/registration/2016gen_party_pa.xlsx'

reg_2016_df = pd.read_excel(reg_2016_file, index_col=0)
reg_2020_df = pd.read_excel(reg_2020_file, index_col=0)


# clean and condense historical vote data
def condense_third_parties(row):
    if row['Party Name'] == 'Republican':
        val = 'REP'
    elif row['Party Name'] == 'Democratic':
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

def define_county(row):
    if (row['WINNER_12_raw'] == 1) & (row['WINNER_16_raw'] == 1):
        val = 'SOLID BLUE'
    elif (row['WINNER_12_raw'] == 2) & (row['WINNER_16_raw'] == 2):
        val = 'SOLID RED'
    elif (row['WINNER_12_raw'] == 1) & (row['WINNER_16_raw'] == 2):
        val = 'OBAMA-TRUMP'
    elif (row['WINNER_12_raw'] == 2) & (row['WINNER_16_raw'] == 1):
        val = 'ROMNEY-CLINTON'
    return val


concatenated_df['PartyCode3'] = concatenated_df.apply(condense_third_parties, axis=1)

concatenated_df.columns = concatenated_df.columns.str.replace(' ', '_')
concatenated_df['Votes'] = concatenated_df.Votes.str.replace(',', '').astype(float)

# create pivot tables for previous years by county -- one raw data and one % row data for 2012 2016 

potus_12_raw = pd.pivot_table(concatenated_df[(concatenated_df.Office_Name == 'President of the United States') & \
                                             (concatenated_df.Election_Name == '2012 General Election')], \
                            index=['County_Name'], columns='PartyCode3', values='Votes', \
                            aggfunc=np.sum)

potus_12_pct = pd.pivot_table(concatenated_df[(concatenated_df.Office_Name == 'President of the United States') & \
                                             (concatenated_df.Election_Name == '2012 General Election')], \
                            index=['County_Name'], columns='PartyCode3', values='Votes', \
                            aggfunc=np.sum).apply(lambda x: x/x.sum(), axis=1).round(2)

potus_16_raw = pd.pivot_table(concatenated_df[(concatenated_df.Office_Name == 'President of the United States') & \
                                             (concatenated_df.Election_Name == '2016 Presidential Election')], \
                            index=['County_Name'], columns='PartyCode3', values='Votes', \
                            aggfunc=np.sum)

potus_16_pct = pd.pivot_table(concatenated_df[(concatenated_df.Office_Name == 'President of the United States') & \
                                             (concatenated_df.Election_Name == '2016 Presidential Election')], \
                            index=['County_Name'], columns='PartyCode3', values='Votes', \
                            aggfunc=np.sum).apply(lambda x: x/x.sum(), axis=1).round(2)

# create net vote variable
potus_12_raw['NET_DEM'] = potus_12_raw["DEM"] - potus_12_raw["REP"]
potus_12_pct['NET_DEM'] = potus_12_pct["DEM"] - potus_12_pct["REP"]
potus_16_raw['NET_DEM'] = potus_16_raw["DEM"] - potus_16_raw["REP"]
potus_16_pct['NET_DEM'] = potus_16_pct["DEM"] - potus_16_pct["REP"]




# Winner key: 1 = Dem, 0 = Rep
potus_12_raw['WINNER'] = potus_12_raw.apply(county_winner, axis=1)
potus_12_pct['WINNER'] = potus_12_pct.apply(county_winner, axis=1)
potus_16_raw['WINNER'] = potus_16_raw.apply(county_winner, axis=1)
potus_16_pct['WINNER'] = potus_16_pct.apply(county_winner, axis=1)

# rename columns in individual df's to allow me to combine them all to one big table
orig_col_list = ['DEM', 'OTHER', 'REP', 'NET_DEM', 'WINNER']


potus_12_raw.columns = rename_columns(orig_col_list, '_12_raw')
potus_12_pct.columns = rename_columns(orig_col_list, '_12_pct')
potus_16_raw.columns = rename_columns(orig_col_list, '_16_raw')
potus_16_pct.columns = rename_columns(orig_col_list, '_16_pct')

full_table = pd.concat([potus_12_raw,potus_12_pct,potus_16_raw,potus_16_pct], axis=1)

full_table['COUNTY_CAT'] = full_table.apply(define_county, axis=1)

full_table['TREND_12_16'] = full_table['NET_DEM_16_pct'].subtract(full_table['NET_DEM_12_pct']).round(2)


# one of the datasets had extra whitespace in the column names so now I'm just doing this for all of them :-\
reg_2016_df.index = reg_2016_df.index.str.strip()
reg_2020_df.index = reg_2020_df.index.str.strip()

# cleaning up columns to add to full table
reg_2016_df['Reg_16_Total'] = reg_2016_df['Total']
reg_2016_df['Reg_16_Rep'] = reg_2016_df['Republican']
reg_2016_df['Reg_16_Dem'] = reg_2016_df['Democratic']
reg_2016_df['Reg_16_Other'] = reg_2016_df['Other']

reg_2020_df['Reg_20_Total'] = reg_2020_df['Total']
reg_2020_df['Reg_20_Rep'] = reg_2020_df['Republican']
reg_2020_df['Reg_20_Dem'] = reg_2020_df['Democratic']
reg_2020_df['Reg_20_Other'] = reg_2020_df['Other']

# drop original columns
reg_2016_df = reg_2016_df.drop(reg_2016_df.columns[0:4], axis=1)
reg_2020_df = reg_2020_df.drop(reg_2020_df.columns[0:4], axis=1)

full_table_reg = pd.concat([full_table,region_df,reg_2016_df,reg_2020_df], axis=1)

full_table_reg['Total_Vote_16'] = full_table_reg['DEM_16_raw'] + full_table_reg['REP_16_raw'] + full_table_reg['OTHER_16_raw']

full_table_reg['Turnout_16'] = (full_table_reg['Total_Vote_16'] / full_table_reg ['Reg_16_Total']).round(3)

# Expected 2020 Vote Totals based on 2020 Registration and 2016 Turnout
full_table_reg['Expected_2020_Vote'] = (full_table_reg['Reg_20_Total'] * full_table_reg['Turnout_16']).round(0)

full_table_reg.to_csv("penn_hist.csv", index_label='County') 
