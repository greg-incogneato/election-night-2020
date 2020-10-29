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
from zipfile import ZipFile

s3 = boto3.resource('s3')

url_NC_live = 'http://dl.ncsbe.gov/ENRS/2020_11_03/results_pct_20201103.zip'
url_NC = 'https://en2020.s3.amazonaws.com/NC_data_live.txt'

filebytes = BytesIO(get_zip_data())
myzipfile = zipfile.ZipFile(filebytes)

resp = urlopen("http://www.test.com/file.zip")
zipfile = ZipFile(BytesIO(resp.read()))
for line in zipfile.open(file).readlines():
    print(line.decode('utf-8'))


zip_obj = s3_resource.Object(bucket_name="en2020", key=zip_key)
buffer = BytesIO(zip_obj.get()["Body"].read())

z = zipfile.ZipFile(buffer)
file_info = z.getinfo(url_NC_live)
s3_resource.meta.client.upload_fileobj(
    z.open(url_NC_live),
    Bucket=en2020,
    Key=f'{url_NC_live}'
)


def update_NC(input_url):
    election_night_data_NC = read_csv_regex(input_url, ['ElectionDate'])
    
    # write FL data from election feed to s3
    csv_buffer_NC_live = StringIO()
    election_night_data_NC.to_csv(csv_buffer_NC_live)
    s3.Object('en2020', 'NC_data_live.txt').put(Body=csv_buffer_NC_live.getvalue(), ACL='public-read')

    return election_night_data_NC
