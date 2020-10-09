# -*- coding: utf-8 -*-

# Run this app with `python app.py` and
# visit http://127.0.0.1:8050/ in your web browser.

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

# re-scan the dashboard data files to pick up updates
# set to load every 60 seconds; data will refresh every 1 minutes
# number of seconds between re-calculating the data                                                                                                                           
UPDATE_INTERVAL = 60

def get_new_data_PA():
    """Updates the global variable 'data' with new data"""
    global EN_PA_df
    EN_PA_df = pd.read_csv('https://en2020.s3.amazonaws.com/penn_dash.csv')
    EN_PA_df['County'] = EN_PA_df.CountyName

def get_new_data_FL():
    """Updates the global variable 'data' with new data"""
    global EN_FL_df
    EN_FL_df = pd.read_csv('https://en2020.s3.amazonaws.com/florida_dash.csv')
    EN_FL_df['County'] = EN_FL_df.CountyName  

def get_new_data_every(period=UPDATE_INTERVAL):
    """Update the data every 'period' seconds"""
    while True:
        # print("updating....")
        # refresh_live_data()
        # print('data refreshed')
        get_new_data_PA()
        get_new_data_FL()
        timestamp = datetime.now().strftime("%I:%M%p %z %b %d %Y")
        print("data updated %s" % timestamp)
        time.sleep(period)

get_new_data_PA()
get_new_data_FL()
# Run the function in another thread
executor = ThreadPoolExecutor(max_workers=1)
executor.submit(get_new_data_every)
###############################################



with urlopen('https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json') as response:
    counties = json.load(response)

external_stylesheets = ['https://cdn.jsdelivr.net/gh/kognise/water.css@latest/dist/light.min.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
server = app.server

def stacked_bars(df):
    biden_vote_df = pd.DataFrame(df[['DEM_20_raw','COUNTY_CAT','County']])
    biden_vote_df.columns = ['Votes','Category','County']
    biden_vote_df['Candidate'] = "Biden"

    trump_vote_df = pd.DataFrame(df[['REP_20_raw','COUNTY_CAT','County']])
    trump_vote_df.columns = ['Votes','Category','County']
    trump_vote_df['Candidate'] = "Trump"

    other_vote_df = pd.DataFrame(df[['OTHER_20_raw','COUNTY_CAT','County']])
    other_vote_df.columns = ['Votes','Category','County']
    other_vote_df['Candidate'] = "Other"

    remaining_vote_df = pd.DataFrame(df[['Expected_20_Vote_Remaining','COUNTY_CAT','County']])
    remaining_vote_df.columns = ['Votes','Category','County']
    remaining_vote_df['Candidate'] = "Remaining"

    stacked_bar_df = pd.concat([biden_vote_df,trump_vote_df,other_vote_df,remaining_vote_df], axis=0)

    fig = px.bar(stacked_bar_df, x='Candidate', y="Votes", color="Category", 
                 color_discrete_map={"SOLID BLUE":"rgb(51,153,255)", "SOLID RED":"rgb(255,102,102)",
                                    "OBAMA-TRUMP":'#AB63FA',"ROMNEY-CLINTON":'#FFA15A'},
                 hover_name="County", hover_data={
                    'County':True,
                    'Votes':':,.0f',
                    'Candidate':False
                    },
                 title="Vote Breakdown by County",
                 template='plotly_white')

    fig.update_layout(
        hoverlabel=dict(
            bgcolor="white",
            font_size=14,
            font_family="Rockwell"
        )
    )            

    return fig

def bubbles(df):
    fig = px.scatter(df, x="NET_DEM_20_pct", y="NET_DEM_16_pct",
                     size="Total_Vote_20", color="COUNTY_CAT",
                     color_discrete_map={ # replaces default color mapping by value
                        "SOLID BLUE":"rgb(51,153,255)", "SOLID RED":"rgb(255,102,102)",
                         "OBAMA-TRUMP":'#AB63FA',"ROMNEY-CLINTON":'#FFA15A'
                        },
                     hover_name="County", size_max=60,
                     labels={'COUNTY_CAT':'Category',
                        'NET_DEM_20_pct':'Biden Margin',
                        'NET_DEM_16_pct':'Clinton Margin',
                        'Expected_20_Vote_Remaining':'Expected Votes Remaining',
                        'Proj_Winner':'Projected Winner',
                        'Total_Vote_20':'Total Votes Counted'
                     },
                    hover_data={
                       'COUNTY_CAT':True,
                       'NET_DEM_20_pct':':+.3p',
                       'NET_DEM_16_pct':':+.3p',
                       'Total_Vote_20':':,.0f',
                       'Expected_20_Vote_Remaining':':,.0f',
                       'Proj_Winner':True
                     },
                     template='plotly_white')                 

    fig.update_layout(
        hoverlabel=dict(
            bgcolor="white",
            font_size=14,
            font_family="Rockwell"
        )
    )
    
    return fig

def choropleth(df, lat, lon, zoom):
    fig = px.choropleth_mapbox(df, geojson=counties, locations='FIPS', color='NET_DEM_20_pct',
                               color_continuous_scale="RdBu",
                               range_color=(-1, 1),
                               mapbox_style="carto-positron",
                               zoom=zoom, center = {"lat": lat, "lon": lon},
                               opacity=0.9,
                               hover_data={'FIPS':False,
                                           'County':True,
                                           'COUNTY_CAT':True,
                                           'NET_DEM_20_pct':':+.3p',
                                           'Total_Vote_20':':,.0f',
                                           'Expected_20_Vote_Remaining':':,.0f',
                                           'Proj_Winner':True
                                          },
                               labels={'NET_DEM_20_pct':'Biden Margin',
                                      'COUNTY_CAT':'Category',
                                      'Total_Vote_20':'Votes Counted',
                                      'Expected_20_Vote_Remaining':'Expected Votes Remaining',
                                      'Proj_Winner':'Projected Winner'}
                              )

    fig.update_layout(
        margin={"r":0,"t":0,"l":0,"b":0}
    )

    fig.update_layout(
        hoverlabel=dict(
            bgcolor="white",
            font_size=14,
            font_family="Rockwell"
        )
    )
    
    return fig


#####
## Call each graph with a app.callback function

PA_map_detail = [41.203323,-77.194527,6]
FL_map_detail = [27.664827,-81.515755,5]


@app.callback(Output('florida-map', 'figure'),
              [Input('interval-component', 'n_intervals')])
def fl_map(n):
    map_FL = choropleth(EN_FL_df, FL_map_detail[0], FL_map_detail[1], FL_map_detail[2])
    return map_FL

@app.callback(Output('penn-map', 'figure'),
              [Input('interval-component', 'n_intervals')])
def pa_map(n):
    map_PA = choropleth(EN_PA_df, PA_map_detail[0], PA_map_detail[1], PA_map_detail[2])
    return map_PA

@app.callback(Output('penn-stacked', 'figure'),
              [Input('interval-component', 'n_intervals')])
def penn_stacked(n):
    stacked_fig_PA = stacked_bars(EN_PA_df)
    return stacked_fig_PA

@app.callback(Output('florida-stacked', 'figure'),
              [Input('interval-component', 'n_intervals')])
def fl_stacked(n):
    stacked_fig_FL = stacked_bars(EN_FL_df)
    return stacked_fig_FL

@app.callback(Output('florida-bubbles', 'figure'),
              [Input('interval-component', 'n_intervals')])
def fl_bubbles(n):
    bubbles_fig_FL = bubbles(EN_FL_df)
    return bubbles_fig_FL

@app.callback(Output('penn-bubbles', 'figure'),
              [Input('interval-component', 'n_intervals')])
def penn_bubbles(n):
    bubbles_fig_PA = bubbles(EN_PA_df)
    return bubbles_fig_PA

# map_PA = choropleth(EN_PA_df, PA_map_detail[0], PA_map_detail[1], PA_map_detail[2])
# map_FL = choropleth(EN_FL_df, FL_map_detail[0], FL_map_detail[1], FL_map_detail[2])
# stacked_fig_PA = stacked_bars(EN_PA_df)
# stacked_fig_FL = stacked_bars(EN_FL_df)
# bubbles_fig_FL = bubbles(EN_FL_df)
# bubbles_fig_PA = bubbles(EN_PA_df)


def make_layout():
    return html.Div(children=[
        html.H1(children='2020 Election Night Dashboard'),
            html.Div([dcc.Markdown(
                """
                Welcome to my geocities page. I will update this text later with some detail of what you are seeing.
                """
            ),
                html.P([html.Small("You might find some more context on my twitter "), html.A(html.Small("@grackle_shmackl"), href="https://twitter.com/grackle_shmackl", title="twitter"), html.Small(".")]),
        ]),
        html.Div(id='live-update-text'),
        html.Div(children='''
            Florida.
        '''),
        dcc.Graph(
            id='florida-map'#,
            #figure=map_FL
        ),
        dcc.Graph(
            id='florida-bubbles'#,
            # figure=bubbles_fig_FL
        ),
        dcc.Graph(
            id='florida-stacked'#,
            # figure=stacked_fig_FL
        ),
        html.Div(children='''
            Pennsylvania.
        '''),    
        dcc.Graph(
            id='penn-map'#,
            # figure=map_PA
        ),
        dcc.Graph(
            id='penn-bubbles'#,
            # figure=bubbles_fig_PA
        ),
        dcc.Graph(
            id='penn-stacked'#,
            # figure=stacked_fig_PA
        ),
        dcc.Interval(
            id='interval-component',
            interval=1*1000*60*2,#*60*5, # 5 minutes in milliseconds
            n_intervals=0
        ),
        html.Div([dcc.Markdown(
            """
            Closing text and detail here.
            """  
        )]),

    ])

app.title = 'Election 2020 Dashboard'
app.layout = make_layout
# @app.callback(Output('live-update-text', 'children'),
#               [Input('interval-component', 'n_intervals')])
# def update_time_and_data(n):
    # timestamp = datetime.now().strftime("%I:%M%p %z %b %d %Y")

    # un-comment these rows to resume live updates from the web!!
    #election_night_data_FL = update_FL(url_FL)
    #election_night_data_PA = update_PA(url_PA)

    # process election night data into full table
    #full_table_EN_FL = process_live_file(election_night_data_FL, fl_hist_df, "FL")
    #full_table_EN_PA = process_live_file(election_night_data_PA, penn_hist_df, 'PA')

    #full_table_EN_FL.to_csv("florida_dash.csv", index_label='CountyName') 
    #full_table_EN_PA.to_csv("penn_dash.csv", index_label='CountyName') 
    ## end un-comment

    # return "Updates every 5 minutes. Last updated: %s" % timestamp

if __name__ == '__main__':
    app.run_server(debug=True)
