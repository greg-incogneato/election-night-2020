# -*- coding: utf-8 -*-

# Run this app with `python app.py` and
# visit http://127.0.0.1:8050/ in your web browser.

import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.express as px
import pandas as pd
from dash.dependencies import Input, Output
from urllib.request import urlopen
import json

EN_FL_df = pd.read_csv('florida_dash.csv')
EN_FL_df['County'] = EN_FL_df.CountyName

EN_PA_df = pd.read_csv('penn_dash.csv')
EN_PA_df['County'] = EN_PA_df.CountyName

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

PA_map_detail = [41.203323,-77.194527,6]
FL_map_detail = [27.664827,-81.515755,5]

map_FL = choropleth(EN_FL_df, FL_map_detail[0], FL_map_detail[1], FL_map_detail[2])
map_PA = choropleth(EN_PA_df, PA_map_detail[0], PA_map_detail[1], PA_map_detail[2])

stacked_fig_FL = stacked_bars(EN_FL_df)
stacked_fig_PA = stacked_bars(EN_PA_df)

bubbles_fig_FL = bubbles(EN_FL_df)
bubbles_fig_PA = bubbles(EN_PA_df)

app.layout = html.Div(children=[
    html.H1(children='2020 Election Night Dashboard'),

    html.Div(children='''
        Florida.
    '''),
    dcc.Graph(
        id='florida-map',
        figure=map_FL
    ),
    dcc.Graph(
        id='florida-bubbles',
        figure=bubbles_fig_FL
    ),
    dcc.Graph(
        id='florida-stacked',
        figure=stacked_fig_FL
    ),
    html.Div(children='''
        Pennsylvania.
    '''),    
    dcc.Graph(
        id='penn-map',
        figure=map_PA
    ),
    dcc.Graph(
        id='penn-bubbles',
        figure=bubbles_fig_PA
    ),
    dcc.Graph(
        id='penn-stacked',
        figure=stacked_fig_PA
    ),
])

if __name__ == '__main__':
    app.run_server(debug=True)
