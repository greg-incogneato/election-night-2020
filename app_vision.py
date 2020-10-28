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
import dash_table
from io import StringIO
import io
import json
import numpy as np
import pandas as pd
import plotly.express as px
import requests 
import xml.etree.ElementTree as ET
import time
import pytz
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

s3 = boto3.resource('s3')

# re-scan the dashboard data files to pick up updates
# set to load every 60 seconds; data will refresh every 1 minutes
# number of seconds between re-calculating the data                                                                                                                           
UPDATE_INTERVAL = 60*5

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

def get_new_data_MI():
    """Updates the global variable 'data' with new data"""
    global EN_MI_df
    EN_MI_df = pd.read_csv('https://en2020.s3.amazonaws.com/mich_dash.csv')
    EN_MI_df['County'] = EN_MI_df.CountyName  

def get_new_data_NC():
    """Updates the global variable 'data' with new data"""
    global EN_NC_df
    EN_NC_df = pd.read_csv('https://en2020.s3.amazonaws.com/ncar_dash.csv')
    EN_NC_df['County'] = EN_NC_df.CountyName  

def get_new_data_every(period=UPDATE_INTERVAL):
    """Update the data every 'period' seconds"""
    while True:
        # print("updating....")
        # refresh_live_data()
        # print('data refreshed')
        get_new_data_PA()
        get_new_data_FL()
        get_new_data_MI()
        get_new_data_NC()
        timestamp = datetime.now().strftime("%I:%M%p %z %b %d %Y")
        print("data updated %s" % timestamp)
        time.sleep(period)

# get_new_data_PA()
# get_new_data_FL()
# Run the function in another thread
executor = ThreadPoolExecutor(max_workers=1)
executor.submit(get_new_data_every)
###############################################


with urlopen('https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json') as response:
    counties = json.load(response)

external_stylesheets = ['https://cdn.jsdelivr.net/npm/water.css@2/out/light.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
server = app.server

def stacked_bars_vision(df, statename):    
        
    biden_vote_df = pd.DataFrame(df[['DEM_20_raw','Proj_Dem_Vote_Remaining']])
    biden_vote_df.columns = ['Votes Counted','Vote Estimate']
    biden_vote_df['Candidate'] = "Biden"

    trump_vote_df = pd.DataFrame(df[['REP_20_raw','Proj_Rep_Vote_Remaining']])
    trump_vote_df.columns = ['Votes Counted','Vote Estimate']
    trump_vote_df['Candidate'] = "Trump"

    other_vote_df = pd.DataFrame(df[['OTHER_20_raw','Proj_Other_Vote_Remaining']])
    other_vote_df.columns = ['Votes Counted','Vote Estimate']
    other_vote_df['Candidate'] = "Other"

    stacked_bar_df = pd.concat([biden_vote_df,trump_vote_df,other_vote_df], axis=0)

    stacked_bar_df = pd.pivot_table(stacked_bar_df, index=['Candidate'],  values=('Votes Counted','Vote Estimate'),
                   aggfunc=np.sum, margins=False).reset_index()
    
    stacked_bar_df = stacked_bar_df.reindex([0,2,1])

    fig = px.bar(stacked_bar_df, x='Candidate', y=(['Votes Counted','Vote Estimate']),
                 color_discrete_map={"Votes Counted":"#14213d", "Vote Estimate":"#fca311"},
                 hover_data={
                    'Candidate':True,
                    'variable':True,
                    'value':':,.0f'
                    },
                 title="%s Vote Estimate" % statename,
                 template='plotly_white')

    fig.update_layout(
        hoverlabel=dict(
            bgcolor="white",
            font_size=14,
            font_family="Rockwell"
        )
    )            

    return fig

@app.callback(Output('penn-vision', 'figure'),
              [Input('interval-component', 'n_intervals')])
def penn_vision(n):
    stacked_fig_PA = stacked_bars_vision(EN_PA_df, "Pennsylvania")
    return stacked_fig_PA

@app.callback(Output('florida-vision', 'figure'),
              [Input('interval-component', 'n_intervals')])
def fl_vision(n):
    stacked_fig_FL = stacked_bars_vision(EN_FL_df, "Florida")
    return stacked_fig_FL

@app.callback(Output('mich-vision', 'figure'),
              [Input('interval-component', 'n_intervals')])
def mi_vision(n):
    stacked_fig_MI = stacked_bars_vision(EN_MI_df, "Michigan")
    return stacked_fig_MI

@app.callback(Output('ncar-vision', 'figure'),
              [Input('interval-component', 'n_intervals')])
def nc_vision(n):
    stacked_fig_NC = stacked_bars_vision(EN_NC_df, "North Carolina")
    return stacked_fig_NC

@app.callback(Output('live-update-text', 'children'),
              [Input('interval-component', 'n_intervals')])
def update_refresh_timestamp(n):
    timezone = pytz.timezone("US/Eastern")
    timestamp = datetime.now().astimezone(timezone).strftime("%I:%M%p US/Eastern %b %d %Y")
    return "Data refreshes every 5 minutes. Last updated: %s" % timestamp

def make_layout():
    return html.Div(children=[
        html.H1(children='Grackle.Live 2020 Election Night Dashboard'),
            html.Div([dcc.Markdown(
                """
                _Grackle Live: Be the Early Bird._ 

                Not sharing this widely because I don't trust it, but here it is!

                For more information, visit the FAQ @ [about.grackle.live](https://about.grackle.live) or scroll down to see the contact details at the bottom of the page.
                """
            )
        ]),
        html.Div(id='live-update-text'),
        dcc.Graph(
            id='florida-vision'
        ),
        dcc.Graph(
            id='penn-vision'
        ),
        dcc.Graph(
            id='mich-vision'
        ),
        dcc.Graph(
            id='ncar-vision'
        ),
        dcc.Interval(
            id='interval-component',
            interval=1*1000*60*5, # 5 minutes in milliseconds
            n_intervals=0
        ),
        html.H2(children=
            """
            Contact Information:

            """  
        ),
        html.P([html.Big("Email: "), html.A(html.Big("grackle@grackle.live"), href="mailto:grackle@grackle.live", title="email")]),
        html.P([html.Big("Twitter: "), html.A(html.Big("@grackle_shmackl"), href="https://twitter.com/grackle_shmackl", title="twitter")]),
        html.P([html.Big("GitHub: "), html.A(html.Big("@greg_incogneato"), href="https://github.com/greg-incogneato", title="github")]),
        html.P([html.Big("FAQ: "), html.A(html.Big("about.grackle.live"), href="https://about.grackle.live", title="faq")])
    ])


app.title = 'Grackle.Live Vision'
app.layout = make_layout


if __name__ == '__main__':
    app.run_server(debug=True)
