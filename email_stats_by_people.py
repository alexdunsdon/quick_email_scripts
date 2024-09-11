import os
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from collections import defaultdict
from datetime import datetime
import pandas as pd

# Define the scope required for accessing Gmail
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
MAX_EMAILS_TO_FETCH = 100000
BATCH_SIZE = 50  # Not used anymore but keeping for compatibility

# OAuth 2.0 authentication function
def authenticate_gmail():
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    return build('gmail', 'v1', credentials=creds)

# Fetch emails using Gmail API
def fetch_emails_by_address(service, email_addresses, max_emails):
    email_stats = defaultdict(lambda: {
        'first_email_date': None,
        'last_email_date': None,
        'total_sent': 0,
        'total_received': 0,
        'total_emails': 0
    })

    for email_address in email_addresses:
        print(f"\nSearching for emails to/from: {email_address}")
        query = f'from:{email_address} OR to:{email_address}'

        results = service.users().messages().list(userId='me', q=query, maxResults=max_emails).execute()
        messages = results.get('messages', [])

        print(f"Found {len(messages)} emails for {email_address}. Processing now...")

        for msg in messages:
            msg_data = service.users().messages().get(userId='me', id=msg['id'], format='metadata').execute()
            headers = msg_data['payload']['headers']

            email_date = None
            from_email = None
            to_email = None

            for header in headers:
                if header['name'] == 'Date':
                    email_date = datetime.strptime(header['value'], '%a, %d %b %Y %H:%M:%S %z')
                elif header['name'] == 'From':
                    from_email = header['value']
                elif header['name'] == 'To':
                    to_email = header['value']

            if email_date:
                if email_stats[email_address]['first_email_date'] is None or email_date < email_stats[email_address]['first_email_date']:
                    email_stats[email_address]['first_email_date'] = email_date
                if email_stats[email_address]['last_email_date'] is None or email_date > email_stats[email_address]['last_email_date']:
                    email_stats[email_address]['last_email_date'] = email_date

            if email_address in from_email:
                email_stats[email_address]['total_sent'] += 1
            if email_address in to_email:
                email_stats[email_address]['total_received'] += 1

            email_stats[email_address]['total_emails'] += 1

    return email_stats

# Summarize email statistics
def summarize_stats(email_stats):
    sorted_emails = sorted(email_stats.items(), key=lambda x: x[1]['total_emails'], reverse=True)
    for email_address, stats in sorted_emails:
        print(f"\nSummary for email: {email_address}")
        print(f"  Total emails sent: {stats['total_sent']}")
        print(f"  Total emails received: {stats['total_received']}")
        print(f"  Total emails exchanged: {stats['total_emails']}")
        print(f"  First email: {stats['first_email_date'].strftime('%Y-%m-%d') if stats['first_email_date'] else 'N/A'}")
        print(f"  Last email: {stats['last_email_date'].strftime('%Y-%m-%d') if stats['last_email_date'] else 'N/A'}")

# Save the statistics to a CSV file
def save_stats_to_csv(email_stats, filename="email_stats.csv"):
    data = []
    for email_address, stats in email_stats.items():
        data.append({
            "Email Address": email_address,
            "Sent": stats['total_sent'],
            "Received": stats['total_received'],
            "Total": stats['total_emails'],
            "First Email": stats['first_email_date'].strftime('%Y-%m-%d') if stats['first_email_date'] else None,
            "Last Email": stats['last_email_date'].strftime('%Y-%m-%d') if stats['last_email_date'] else None
        })

    df = pd.DataFrame(data)
    df.to_csv(filename, index=False)
    print(f"CSV file saved to {filename}")

# Main function
def main():
    email_addresses = ["example1@gmail.com", "example2@gmail.com"]  # Modify this with real email addresses

    service = authenticate_gmail()  # Use OAuth to authenticate with Gmail
    if service:
        email_stats = fetch_emails_by_address(service, email_addresses, MAX_EMAILS_TO_FETCH)

        # Summarize and save stats
        summarize_stats(email_stats)
        save_stats_to_csv(email_stats, filename="email_stats.csv")

if __name__ == "__main__":
    main()
    