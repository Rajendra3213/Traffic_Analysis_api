import json
import pandas as pd
import re
import tldextract
import logging
from celery import Celery

# Configure logging
logging.basicConfig(level=logging.INFO)

# Celery app configuration
app = Celery('Analyze', broker='redis://localhost:6379/0', backend='redis://localhost:6379/0')
app.conf.timezone = 'Asia/Kathmandu'

@app.task
def analyzeData():
    try:
        logging.info("Analyzing data..")
        data = pd.read_csv("/Users/rajendraacharya/Desktop/log/Network_Analysis/log.csv")
        data['session_id'] = data['session_id'].str.extract(r'SID-(.*?)\-\d+')
        session_ids = data['session_id'].unique()

        result = process_data(data, session_ids[0])
        return json.loads(result)
    except Exception as e:
        logging.error(f"Error in analyzeData: {e}")
        raise

@app.task
def singleSessionAnalysis(sessionID: str):
    try:
        logging.info(f"Analyzing data for session: {sessionID}")
        data = pd.read_csv("/Users/rajendraacharya/Desktop/log/Network_Analysis/log.csv")
        data['session_id'] = data['session_id'].str.extract(r'SID-(.*?)\-\d+')

        result = process_data(data, sessionID)
        return json.loads(result)
    except Exception as e:
        logging.error(f"Error in singleSessionAnalysis: {e}")
        raise

def extract_http_url(misc):
    match = re.search(r'HttpUrl=([^ ]+)', misc)
    return match.group(1) if match else None

def extract_primary_domain_name(full_url):
    ext = tldextract.extract(full_url)
    return ext.domain

def calculate_time_spent_on_urls(data, session_id):
    data.columns = data.columns.str.strip()
    data['datetime'] = pd.to_datetime(data['date'] + ' ' + data['time'])
    filtered_data = data[data['session_id'] == session_id].copy()
    filtered_data['HttpUrl'] = filtered_data['misc'].apply(extract_http_url)
    filtered_data = filtered_data.dropna(subset=['HttpUrl'])
    filtered_data['PrimaryDomainName'] = filtered_data['HttpUrl'].apply(extract_primary_domain_name)
    filtered_data = filtered_data.sort_values(by='datetime')
    filtered_data['time_diff_seconds'] = filtered_data['datetime'].diff().dt.total_seconds().fillna(0)
    filtered_data['time_diff_hours'] = filtered_data['time_diff_seconds'] / 3600
    filtered_data.iloc[0, filtered_data.columns.get_loc('time_diff_seconds')] = 0
    filtered_data.iloc[0, filtered_data.columns.get_loc('time_diff_hours')] = 0
    result = filtered_data[['datetime', 'HttpUrl', 'PrimaryDomainName', 'time_diff_hours']]
    result['datetime'] = result['datetime'].astype(str)  # Convert Timestamp to string
    domain_time_spent = result.groupby('PrimaryDomainName').agg(
        total_time_spent_hours=('time_diff_hours', 'sum')
    ).reset_index()
    top_domains = domain_time_spent.nlargest(5, 'total_time_spent_hours')
    return result.head(20), domain_time_spent.head(20), top_domains.head(20)

def calculate_packets_sent(data, session_id):
    data.columns = data.columns.str.strip()
    filtered_data = data[data['session_id'] == session_id].copy()
    filtered_data['HttpUrl'] = filtered_data['misc'].apply(extract_http_url)
    filtered_data = filtered_data.dropna(subset=['HttpUrl'])
    filtered_data['PrimaryDomainName'] = filtered_data['HttpUrl'].apply(extract_primary_domain_name)
    domain_packet_info = filtered_data.groupby('PrimaryDomainName').agg(
        TotalPacketsSent=('PrimaryDomainName', 'size'),
        TotalPacketSize=('packet_size', 'sum')
    ).reset_index()
    top_domains = domain_packet_info.nlargest(5, 'TotalPacketsSent')
    return domain_packet_info.head(20), top_domains.head(20)

def process_data(data, session_id):
    if 'session_id' in data.columns:
        filtered_urls, domain_time_spent, top_time_domains = calculate_time_spent_on_urls(data, session_id)
        domain_packet_info, top_packet_domains = calculate_packets_sent(data, session_id)
        result = {
            'time_spent': {
                'filtered_urls': filtered_urls.to_dict(orient='records'),
                'domain_time_spent': domain_time_spent.to_dict(orient='records'),
                'top_time_domains': top_time_domains.to_dict(orient='records')
            },
            'packets_sent': {
                'domain_packet_info': domain_packet_info.to_dict(orient='records'),
                'top_packet_domains': top_packet_domains.to_dict(orient='records')
            }
        }
        return json.dumps(result, indent=4)
    else:
        raise ValueError('The uploaded file does not contain a "session_id" column.')

if __name__ == "__main__":
    app.start()
