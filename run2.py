import pandas as pd
import glob
import datetime

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

datapath = './Data'
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
    result['change_ratio'] = result['investment_change']/10e6/abs(result['underlying_price_change'])
    return result

def get_price_range():
    files = get_files(datapath)
    data_in = pd.read_json(files[-1])
    data_out = extract_data(data_in)
    expiration_dates = list(data_out['date'].unique())
    price_low = []
    price_high = []
    for date in expiration_dates:
        call_data = data_out[(data_out['date']==date) & (data_out['optiontype']=='call')].sort_values('impact', ascending=False).reset_index(drop=True)
        call_invested = call_data['impact'].sum()
        call_data['impact_percent'] = call_data['impact'].apply(lambda x: x/call_invested)
        call_data['impact_price'] = call_data.apply(lambda x: x.impact_percent*float(x.strike), axis=1)
        price_low.append(call_data['impact_price'].sum())

        put_data = data_out[(data_out['date']==date) & (data_out['optiontype']=='put')].sort_values('impact', ascending=False).reset_index(drop=True)
        put_invested = put_data['impact'].sum()
        put_data['impact_percent'] = put_data['impact'].apply(lambda x: x/put_invested)
        put_data['impact_price'] = put_data.apply(lambda x: x.impact_percent*float(x.strike), axis=1)
        price_high.append(put_data['impact_price'].sum())
    price_range = pd.DataFrame()
    price_range['date'] = expiration_dates
    price_range['low']  = price_low
    price_range['high'] = price_high
    return price_range

files = get_files(datapath)
historical_investment = get_total_investment(files)

data_in = pd.read_json(files[-1])
data_out = extract_data(data_in)


top_options = data_out.sort_values('impact', ascending=False).head(10).reset_index(drop=True)

top_calls = data_out[data_out['optiontype']=='call'].sort_values('total_investment', ascending=False).reset_index(drop=True)

top_puts = data_out[data_out['optiontype']=='put'].sort_values('total_investment', ascending=True).reset_index(drop=True)

data_in = pd.read_json(files[-2])
d2 = extract_data(data_in)

data_out = data_out[['description', 'total_investment']]
d2 = d2[['description', 'total_investment']]
merged_df = pd.merge(data_out, d2, on='description', suffixes=("_before", "_after"))
merged_df['change'] = merged_df.apply(lambda x: x.total_investment_after-x.total_investment_before, axis=1)

top_delta = merged_df.sort_values('change', ascending=False).head(10).reset_index(drop=True)

bottom_delta = merged_df.sort_values('change', ascending=True).head(10).reset_index(drop=True)

price_range = get_price_range()

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

config = pd.read_json('./Data/config.json', typ='series')
recipients = pd.read_json('./Data/mail_to.json', typ='series')
timestamp = datetime.datetime.now().strftime("%Y-%m-%d")
botmail = config["botmail"]
botpw = config["botpw"]
subject = 'AMZN Report - ' + timestamp

def create_msg(historical_investment, top_options, top_calls, top_puts, top_delta, bottom_delta):
    report = '<h>Price Range</h>'
    report += price_range.to_html()
    
    report += '<br><br>'
    report += '<h>Historical Investment</h>'
    report += historical_investment.to_html()
    
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

for recipient in recipients:
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = botmail
    msg['To'] = recipient

#     report = create_msg(historical_investment, top_calls, top_puts, top_delta, bottom_delta)
    report = create_msg(historical_investment, top_options, top_calls, top_puts, top_delta, bottom_delta)
    msg.attach(MIMEText(report, 'html'))

    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.ehlo()
    server.starttls()
    server.login(botmail, botpw)

    server.sendmail(botmail, recipient, msg.as_string())
    server.quit()
    
    