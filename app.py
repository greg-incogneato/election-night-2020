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


def tall_EN_df(df):
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
    
    table_df = pd.concat([biden_vote_df,trump_vote_df,other_vote_df], axis=0)
    return table_df

def tall_EN_df_reg(df):
    reg_df = pd.DataFrame(df[['Reg_20_Total','COUNTY_CAT','County']])
    reg_df.columns = ['Votes','Category','County']
    reg_df['Category'] = 'Registered Voters'
    
    expected_df = pd.DataFrame(df[['Expected_2020_Vote','COUNTY_CAT','County']])
    expected_df.columns = ['Votes','Category','County']
    expected_df['Category'] = 'Expected Votes'

    remaining_df = pd.DataFrame(df[['Expected_20_Vote_Remaining','COUNTY_CAT','County']])
    remaining_df.columns = ['Votes','Category','County']
    remaining_df['Category'] = 'Remaining Votes'

    table_df = pd.concat([reg_df,expected_df,remaining_df], axis=0)
    return table_df

def make_table(state1, state2):
    PA_summary_table = tall_EN_df(state1)
    FL_summary_table = tall_EN_df(state2)

    PA_summary_table['State'] = 'Pennsylvania'
    FL_summary_table['State'] = 'Florida'

    summary_table = pd.concat([FL_summary_table,PA_summary_table])

    summary_table = pd.pivot_table(summary_table, index=['Candidate'],  values=('Votes'), \
                                columns=('State'), aggfunc=np.sum, margins=False).reset_index()
    summary_table = summary_table.reindex([0,2,1])

    summary_table['FL_pct'] = (summary_table.Florida / summary_table.Florida.sum() * 100)
    summary_table['PA_pct'] = (summary_table.Pennsylvania / summary_table.Pennsylvania.sum() * 100)

    summary_table = summary_table.append(summary_table.sum(numeric_only=True), ignore_index=True)
    summary_table.at[3, 'Candidate'] = 'Total'

    summary_table['Florida'] = summary_table['Florida'].map("{:,.0f}".format)
    summary_table['Pennsylvania'] = summary_table['Pennsylvania'].map("{:,.0f}".format)
    summary_table['FL_pct'] = summary_table['FL_pct'].map('{:,.2f}%'.format)
    summary_table['PA_pct'] = summary_table['PA_pct'].map('{:,.2f}%'.format)

    summary_table = summary_table[['Candidate','Florida','FL_pct','Pennsylvania','PA_pct']]

    return summary_table
    # fig = go.Figure(data=[go.Table(
    #     header=dict(values=list(summary_table.columns),
    #                 fill_color='paleturquoise',
    #                 align='left'),
    #     cells=dict(values=[summary_table.Candidate,summary_table.Florida,summary_table.FL_pct,
    #                       summary_table.Pennsylvania,summary_table.PA_pct],
    #                fill_color='lavender',
    #                align='left'))
    # ])

    # fig.show()

def make_vote_table(state1,state2):
    PA_reg_table = tall_EN_df_reg(state1)
    FL_reg_table = tall_EN_df_reg(state2)

    PA_reg_table['State'] = 'Pennsylvania'
    FL_reg_table['State'] = 'Florida'

    reg_table = pd.concat([FL_reg_table,PA_reg_table])

    reg_table = pd.pivot_table(reg_table, index=['Category'],  values=('Votes'), \
                                columns=('State'), aggfunc=np.sum, margins=False).reset_index()

    reg_table['Florida'] = reg_table['Florida'].map("{:,.0f}".format)
    reg_table['Pennsylvania'] = reg_table['Pennsylvania'].map("{:,.0f}".format)

    FL_turnout = str(((EN_FL_df['Total_Vote_16'].sum() / EN_FL_df['Reg_16_Total'].sum())*100).round(1)) + "%"
    PA_turnout = str(((EN_PA_df['Total_Vote_16'].sum() / EN_PA_df['Reg_16_Total'].sum())*100).round(1)) + "%"

    turnout = pd.DataFrame([['2016 Turnout',FL_turnout,PA_turnout]], columns = ['Category','Florida','Pennsylvania'])
    reg_table = reg_table.append(turnout)

    reg_table = reg_table.reset_index(drop=True)
    return reg_table

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

def bubbles(df, y_year):
    if y_year == '2012':
        y_data = 'NET_DEM_12_pct'
        y_label = 'Obama Margin'
    elif y_year == '2016':
        y_data = 'NET_DEM_16_pct'
        y_label = 'Clinton Margin'

    fig = px.scatter(df, x="NET_DEM_20_pct", y=y_data,
                     size="Total_Vote_20", color="COUNTY_CAT",
                     color_discrete_map={ # replaces default color mapping by value
                        "SOLID BLUE":"rgb(51,153,255)", "SOLID RED":"rgb(255,102,102)",
                         "OBAMA-TRUMP":'#AB63FA',"ROMNEY-CLINTON":'#FFA15A'
                        },
                     hover_name="County", size_max=60,
                     labels={'COUNTY_CAT':'Category',
                        'NET_DEM_20_pct':'Biden Margin',
                        y_data:y_label,
                        'Expected_20_Vote_Remaining':'Expected Votes Remaining',
                        # 'Proj_Winner':'Projected Winner',
                        'Total_Vote_20':'Total Votes Counted'
                     },
                    hover_data={
                       'COUNTY_CAT':True,
                       'NET_DEM_20_pct':':+.3p',
                       y_data:':+.3p',
                       'Total_Vote_20':':,.0f',
                       'Expected_20_Vote_Remaining':':,.0f',
                       # 'Proj_Winner':True
                     },
                    title="2020 Vote Against %s Margins" % y_year, 
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
                                           # 'Proj_Winner':True
                                          },
                               labels={'NET_DEM_20_pct':'Biden Margin',
                                      'COUNTY_CAT':'Category',
                                      'Total_Vote_20':'Votes Counted',
                                      'Expected_20_Vote_Remaining':'Expected Votes Remaining'}
                                      # 'Proj_Winner':'Projected Winner'}
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



@app.callback([Output("the-table", "data"), Output('the-table', 'columns')],
              [Input('interval-component', 'n_intervals')])
def summary_table(n):
    the_table = make_table(EN_PA_df,EN_FL_df)
    data=the_table.to_dict('records')
    columns=[{"name": i, "id": i} for i in the_table.columns]
    return data, columns

@app.callback([Output("vote-table", "data"), Output('vote-table', 'columns')],
              [Input('interval-component', 'n_intervals')])
def vote_table(n):
    vote_table = make_vote_table(EN_PA_df,EN_FL_df)
    data=vote_table.to_dict('records')
    columns=[{"name": i, "id": i} for i in vote_table.columns]
    return data, columns

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
              [Input('interval-component', 'n_intervals')],
              [Input('fl_radio', 'value')])
def fl_bubbles(n, fl_radio):
    bubbles_fig_FL = bubbles(EN_FL_df, fl_radio)
    return bubbles_fig_FL

@app.callback(Output('penn-bubbles', 'figure'),
              [Input('interval-component', 'n_intervals')],
              [Input('pa_radio', 'value')])
def penn_bubbles(n, pa_radio):
    bubbles_fig_PA = bubbles(EN_PA_df, pa_radio)
    return bubbles_fig_PA

@app.callback(Output('live-update-text', 'children'),
              [Input('interval-component', 'n_intervals')])
def update_refresh_timestamp(n):
    timezone = pytz.timezone("US/Eastern")
    timestamp = datetime.now().astimezone(timezone).strftime("%I:%M%p US/Eastern %b %d %Y")
    return "Data refreshes every 5 minutes. Last updated: %s" % timestamp

# map_PA = choropleth(EN_PA_df, PA_map_detail[0], PA_map_detail[1], PA_map_detail[2])
# map_FL = choropleth(EN_FL_df, FL_map_detail[0], FL_map_detail[1], FL_map_detail[2])
# stacked_fig_PA = stacked_bars(EN_PA_df)
# stacked_fig_FL = stacked_bars(EN_FL_df)
# bubbles_fig_FL = bubbles(EN_FL_df)
# bubbles_fig_PA = bubbles(EN_PA_df)
# the_table = make_table(EN_PA_df,EN_FL_df)


def make_layout():
    return html.Div(children=[
        html.H1(children='Grackle.Live 2020 Election Night Dashboard'),
            html.Div([dcc.Markdown(
                """
                _Grackle.Live will have the best real-time data analytics on Election Night 2020._ 

                On this page you'll find dashboards for Florida and Pennsylvania with live presidential election results.

                For more information, visit the FAQ @ [about.grackle.live](https://about.grackle.live) or scroll down to see the contact details at the bottom of the page.
                """
            )
        ]),
        html.Div(id='live-update-text'),
        dash_table.DataTable(id='the-table', columns=[],
            data=[], style_as_list_view=True,     
            style_header={
                'backgroundColor': 'light-grey',
                'fontWeight': 'bold'},
            style_cell_conditional=[
                {
                    'if': {'column_id': c},
                    'fontWeight': 'bold'
                } for c in ['Candidate']]
        ),
        dash_table.DataTable(id='vote-table', columns=[],
            data=[], style_as_list_view=True,
            style_header={
                'backgroundColor': 'light-grey',
                'fontWeight': 'bold'},
            style_cell_conditional=[
                {
                    'if': {'column_id': c},
                    'fontWeight': 'bold'
                } for c in ['Category']]
        ),
        html.H2(children='''
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
        html.H4(children='''
            Select Y-axis for Bubble Chart:
        '''),    
        dcc.RadioItems(id='fl_radio',
            options=[{'label': i, 'value': i} for i in ['2012', '2016']],
            value='2016',
            labelStyle={'display': 'inline-block'}
        ),  
        dcc.Graph(
            id='florida-stacked'#,
            # figure=stacked_fig_FL
        ),
        html.H2(children='''
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
        html.H4(children='''
            Select Y-axis for Bubble Chart:
        '''),    
        dcc.RadioItems(id='pa_radio',
            options=[{'label': i, 'value': i} for i in ['2012', '2016']],
            value='2016',
            labelStyle={'display': 'inline-block'}
  
        ),
        dcc.Graph(
            id='penn-stacked'#,
            # figure=stacked_fig_PA
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
        html.P([html.Big("GitHub: "), html.A(html.Big("@greg_ingcogneato"), href="https://github.com/greg-incogneato", title="github")]),
        html.P([html.Big("FAQ: "), html.A(html.Big("about.grackle.live"), href="https://about.grackle.live", title="faq")])
    ])




app.title = 'Grackle.Live'
app.layout = make_layout


if __name__ == '__main__':
    app.run_server(debug=True)
