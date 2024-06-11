import pandas as pd
import re
import tldextract

# Function to extract HttpUrl from the 'misc' column
def extract_http_url(misc):
    match = re.search(r'HttpUrl=([^ ]+)', misc)
    return match.group(1) if match else None

# Function to extract the primary domain name from a full URL
def extract_primary_domain_name(full_url):
    ext = tldextract.extract(full_url)
    return ext.domain

# Function to filter URLs by session ID and calculate the time spent on URLs
def calculate_time_spent_on_urls(data, session_id):
    # Strip any leading/trailing spaces from the column names
    data.columns = data.columns.str.strip()

    # Convert date and time columns to a single datetime column
    data['datetime'] = pd.to_datetime(data['date'] + ' ' + data['time'])

    # Filter the DataFrame by session ID
    filtered_data = data[data['session_id'] == session_id]

    # Extract HttpUrl from 'misc' column
    filtered_data['HttpUrl'] = filtered_data['misc'].apply(extract_http_url)

    # Drop rows where HttpUrl is None
    filtered_data = filtered_data.dropna(subset=['HttpUrl'])

    # Extract primary domain name from the HttpUrl column
    filtered_data['PrimaryDomainName'] = filtered_data['HttpUrl'].apply(extract_primary_domain_name)

    # Sort the filtered data by datetime
    filtered_data = filtered_data.sort_values(by='datetime')

    # Calculate the duration between consecutive URL hits (in seconds)
    filtered_data['time_diff_seconds'] = filtered_data['datetime'].diff().dt.total_seconds().fillna(0)

    # Convert time difference to hours
    filtered_data['time_diff_hours'] = filtered_data['time_diff_seconds'] / 3600  # 3600 seconds in an hour

    # Ensure time_diff is positive by resetting the first row's time_diff to 0
    filtered_data.iloc[0, filtered_data.columns.get_loc('time_diff_seconds')] = 0
    filtered_data.iloc[0, filtered_data.columns.get_loc('time_diff_hours')] = 0

    # Extract relevant columns
    result = filtered_data[['datetime', 'HttpUrl', 'PrimaryDomainName', 'time_diff_seconds', 'time_diff_hours']]

    # Calculate total time spent on primary domain names (in seconds and hours)
    domain_time_spent = result.groupby('PrimaryDomainName').agg(
        total_time_spent_seconds=('time_diff_seconds', 'sum'),
        total_time_spent_hours=('time_diff_hours', 'sum')
    ).reset_index()

    # Get the top 5 most used primary domain names
    top_domains = domain_time_spent.nlargest(5, 'total_time_spent_seconds')
    return result, domain_time_spent, top_domains

# Function to filter URLs by session ID and calculate the total packets sent and packet size for each domain
def calculate_packets_sent(data, session_id):
    # Strip any leading/trailing spaces from the column names
    data.columns = data.columns.str.strip()

    # Filter the DataFrame by session ID
    filtered_data = data[data['session_id'] == session_id]

    # Extract HttpUrl from 'misc' column
    filtered_data['HttpUrl'] = filtered_data['misc'].apply(extract_http_url)

    # Drop rows where HttpUrl is None
    filtered_data = filtered_data.dropna(subset=['HttpUrl'])

    # Extract primary domain name from the HttpUrl column
    filtered_data['PrimaryDomainName'] = filtered_data['HttpUrl'].apply(extract_primary_domain_name)

    # Group by domain and calculate total packets sent and packet size
    domain_packet_info = filtered_data.groupby('PrimaryDomainName').agg(
        TotalPacketsSent=('PrimaryDomainName', 'size'),
        TotalPacketSize=('packet_size', 'sum')
    ).reset_index()

    # Get the top 5 primary domain names with the highest number of packets sent
    top_domains = domain_packet_info.nlargest(5, 'TotalPacketsSent')
    return domain_packet_info, top_domains

# Main function to process the data
def process_data(file_path, session_id):
    # Read the CSV data into a DataFrame
    data = pd.read_csv(file_path)

    if 'session_id' in data.columns:
        # Calculate time spent on URLs for the selected session
        filtered_urls, domain_time_spent, top_time_domains = calculate_time_spent_on_urls(data, session_id)

        # Calculate total packets sent and packet size for the selected session
        domain_packet_info, top_packet_domains = calculate_packets_sent(data, session_id)

        return {
            'time_spent': {
                'filtered_urls': filtered_urls,
                'domain_time_spent': domain_time_spent,
                'top_time_domains': top_time_domains
            },
            'packets_sent': {
                'domain_packet_info': domain_packet_info,
                'top_packet_domains': top_packet_domains
            }
        }
    else:
        raise ValueError('The uploaded file does not contain a "session_id" column.')

# Example usage
file_path = '/Users/rajendraacharya/Desktop/log/Network_Analysis/log.csv'
session_id = 'SID-AMRITA.PANTA-[OPENVPN_L3]-2348'

result = process_data(file_path, session_id)
print(result)
