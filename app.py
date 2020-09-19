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

EN_df = pd.read_csv('/Users/greglewis/Desktop/election20/florida_dash.csv')
EN_df['County'] = EN_df.CountyName

with urlopen('https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json') as response:
    counties = json.load(response)


external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

# assume you have a "long-form" data frame
# see https://plotly.com/python/px-arguments/ for more options

fig = px.choropleth_mapbox(EN_df, geojson=counties, locations='FIPS', color='NET_DEM_20_pct',
                           color_continuous_scale="RdBu",
                           range_color=(-1, 1),
                           mapbox_style="carto-positron",
                           zoom=5, center = {"lat": 27.664827, "lon": -81.515755},
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


app.layout = html.Div(children=[
    html.H1(children='2020 Election Night Dashboard'),

    html.Div(children='''
        May God Have Mercy on Our Souls.
    '''),

    dcc.Graph(
        id='example-graph',
        figure=fig
    )
])

if __name__ == '__main__':
    app.run_server(debug=True)