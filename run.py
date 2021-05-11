import pandas as pd
import glob
import datetime

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import pickle
import os
from google_auth_oauthlib.flow import Flow, InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from google.auth.transport.requests import Request
import base64
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

datapath = './Data'
files = glob.glob(datapath+'/*chain*.json')
files.sort()

def extract_data(data_in):
    option_type = data_in.columns[-2:]
    data_out = pd.DataFrame(columns=['optiontype', 'date', 'underlying_price', 'strike', 'description', 'delta', 'openinterest', 'total_investment'])
    underlying_price = data_in['underlyingPrice'][0]
    for option in option_type:
        dates = data_in[option].keys()
        for date in dates:
            openinterest = []
            description = []
            delta = []
            strikes = data_in[option][date].keys()
            for strike in strikes:
                openinterest.append(data_in[option][date][strike][0]['openInterest'])
                description.append(data_in[option][date][strike][0]['description'])
                delta.append(data_in[option][date][strike][0]['delta'])
            temp = pd.DataFrame()
            temp['openinterest'] = openinterest
            temp['strike'] = strikes
            temp['date'] = date.split(':')[0]
            temp['optiontype'] = option[:-10]
            temp['description'] = description
            temp['delta'] = delta
            data_out = data_out.append(temp)
    data_out['underlying_price'] = underlying_price
    data_out['total_investment'] = data_out.apply(lambda x: float(x.delta) * float(x.openinterest) * float(x.underlying_price) * 100, axis=1)
    data_out['impact'] = data_out['total_investment'].apply(lambda x: abs(x))
    return data_out

def get_files(datapath):
    files = glob.glob(datapath+'/*chain*.json')
    files.sort()
    return files

def get_total_investment(files):
    result = pd.DataFrame()
    date = []
    investment = []
    underlying_price = []
    for file in files:
        date.append(file.split('_')[2])
        data_in = pd.read_json(file)
        data_out = extract_data(data_in)
        investment.append(data_out['total_investment'].sum())
        underlying_price.append(data_out['underlying_price'].max())
    result['date'] = date
    result['investment'] = investment
    result['underlying_price'] = underlying_price
    result['investment_change'] = (result['investment'] - result['investment'].shift(1))
    result['underlying_price_change'] = result['underlying_price'] - result['underlying_price'].shift(1)
    result['change_ratio'] = result['investment_change']/10e6/result['underlying_price_change']
    return result

files = get_files(datapath)
historical_investment = get_total_investment(files)
data_in = pd.read_json(files[-1])
data_out = extract_data(data_in)

top_options = data_out.sort_values('impact', ascending=False).head(10).reset_index(drop=True)
top_calls = data_out[data_out['optiontype']=='call'].sort_values('total_investment', ascending=False).reset_index(drop=True)
top_puts = data_out[data_out['optiontype']=='put'].sort_values('total_investment', ascending=True).reset_index(drop=True)

d2_in = pd.read_json(files[-2])
d2 = extract_data(d2_in)
data_out = data_out[['description', 'total_investment']]
d2 = d2[['description', 'total_investment']]
merged_df = pd.merge(data_out, d2, on='description', suffixes=("_before", "_after"))
merged_df['change'] = merged_df.apply(lambda x: x.total_investment_after-x.total_investment_before, axis=1)

top_delta = merged_df.sort_values('change', ascending=False).head(10).reset_index(drop=True)
bottom_delta = merged_df.sort_values('change', ascending=True).head(10).reset_index(drop=True)

config = pd.read_json('./Data/config.json', typ='series')
recipients = pd.read_json('./Data/mail_to.json', typ='series')
timestamp = datetime.datetime.now().strftime("%Y-%m-%d")
botmail = config["botmail"]
botpw = config["botpw"]
subject = 'AMZN Report - ' + timestamp

def create_msg(historical_investment, top_options, top_calls, top_puts, top_delta, bottom_delta):
    report = '<h>Historical Investment</h>'
    report += historical_investment.head(10).to_html()
    
    report += '<br><br>'
    report += '<h>Top Options</h>'
    report += top_options.head(10).to_html()
    
    report += '<br><br>'
    report += '<h>Top Calls</h>'
    report += top_calls.head(10).to_html()

    report += '<br><br>'
    report += '<h>Top Puts</h>'
    report += top_puts.head(10).to_html()

    report += '<br><br>'
    report += '<h>Top Deltas</h>'
    report += top_delta.head(10).to_html()

    report += '<br><br>'
    report += '<h>Bottom Deltas</h>'
    report += bottom_delta.head(10).to_html()
    return report

def Create_Service(client_secret_file, api_name, api_version, *scopes):
    print(client_secret_file, api_name, api_version, scopes, sep='-')
    CLIENT_SECRET_FILE = client_secret_file
    API_SERVICE_NAME = api_name
    API_VERSION = api_version
    SCOPES = [scope for scope in scopes[0]]
    print(SCOPES)

    cred = None

    pickle_file = f'./Data/token_{API_SERVICE_NAME}_{API_VERSION}.pickle'

    if os.path.exists(pickle_file):
        with open(pickle_file, 'rb') as token:
            cred = pickle.load(token)

    if not cred or not cred.valid:
        if cred and cred.expired and cred.refresh_token:
            cred.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
            cred = flow.run_local_server()

        with open(pickle_file, 'wb') as token:
            pickle.dump(cred, token)

    try:
        service = build(API_SERVICE_NAME, API_VERSION, credentials=cred)
        print(API_SERVICE_NAME, 'service created successfully')
        return service
    except Exception as e:
        print('Unable to connect.')
        print(e)
        return None
    
CLIENT_SECRET_FILE = './Data/client_secret.json'
API_NAME = 'gmail'
API_VERSION = 'v1'
SCOPES = ['https://mail.google.com/']
config = pd.read_json('./Data/config.json', typ='series')
recipients = pd.read_json('./Data/mail_to.json', typ='series')
timestamp = datetime.datetime.now().strftime("%Y-%m-%d")
botmail = config["botmail"]
botpw = config["botpw"]
subject = 'AMZN Report - ' + timestamp
service = Create_Service(CLIENT_SECRET_FILE, API_NAME, API_VERSION, SCOPES)

for recipient in recipients:
    emailMsg = create_msg(historical_investment, top_options, top_calls, top_puts, top_delta, bottom_delta)
    mimeMessage = MIMEMultipart()
    mimeMessage['to'] = recipient
    mimeMessage['subject'] = subject
#     mimeMessage.attach(MIMEText(emailMsg, 'plain'))
    mimeMessage.attach(MIMEText(emailMsg, 'html'))
    raw_string = base64.urlsafe_b64encode(mimeMessage.as_bytes()).decode()

    message = service.users().messages().send(userId='me', body={'raw': raw_string}).execute()
    print(message)