import pandas as pd
import os
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import json
import unicodedata
import re
import base64
from bs4 import BeautifulSoup
from email.utils import parsedate_to_datetime, parseaddr
from datetime import datetime, timedelta, time
from dotenv import load_dotenv

load_dotenv()

SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
OUTPUT_FILE = os.getenv("OUTPUT_FILE")
MY_EMAIL = os.getenv("MY_EMAIL")
BLOCKED_SENDERS = os.getenv("BLOCKED_SENDERS").split(",")
CREDENTIALS_FILE = os.getenv("CREDENTIALS_FILE")
TOKEN_FILE = os.getenv("TOKEN_FILE")
script_dir = os.path.dirname(os.path.abspath(__file__))

LESSON_KEYWORDS = [
    'lesson', 'hi jordan', 'book', 'kg', 'tennis', 'schedule', 'wednesday'
]

def authenticate_gmail():
    creds = None
    credentials_file = os.path.join(script_dir, CREDENTIALS_FILE)
    token_path = os.path.join(script_dir, TOKEN_FILE)

    if os.path.exists(token_path): 
        try:
            creds = Credentials.from_authorized_user_file(token_path, SCOPES)
        except (json.JSONDecodeError, ValueError):
            print("⚠️ token.json was invalid or empty. Deleting and starting fresh...")
            os.remove(token_path)
            creds = None

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(credentials_file, SCOPES)
            creds = flow.run_local_server(port=0)

        with open(token_path, 'w') as token:
            token.write(creds.to_json())

    service = build('gmail', 'v1', credentials=creds)
    return service

def most_recent_wednesday(today=None):
    if today is None:
        today = datetime.today()
    days_since_wed = (today.weekday() - 2) % 7
    return today - timedelta(days=days_since_wed)

def fetch_emails(service):
    anchor_wed = most_recent_wednesday()
    after_date = (anchor_wed - timedelta(days=1)).strftime('%Y/%m/%d')
    before_date = (datetime.today() + timedelta(days=1)).strftime('%Y/%m/%d')
    query = f"after:{after_date} before:{before_date}"

    full_messages = []
    page_token = None

    while True:
        results = service.users().messages().list(
            userId='me',
            q=query,
            maxResults=500,
            pageToken=page_token
        ).execute()

        messages = results.get('messages', [])
        for m in messages:
            msg_detail = service.users().messages().get(
                userId='me', id=m['id'], format='full'
            ).execute()
            full_messages.append(msg_detail)

        page_token = results.get('nextPageToken')
        if not page_token:
            break

    return full_messages

def filter_messages_by_anchor(messages, anchor_wed):
    filtered = []
    today = datetime.today().date()
    for msg in messages:
        headers = msg.get('payload', {}).get('headers', [])
        date_header = next((h['value'] for h in headers if h['name'].lower() == 'date'), None)
        if not date_header:
            continue
        try:
            msg_datetime = parsedate_to_datetime(date_header)
            if anchor_wed.date() <= msg_datetime.date() <= today:
                filtered.append(msg)
        except Exception:
            continue
    return filtered

def extract_email_body(payload):
    if 'parts' in payload:
        for part in payload['parts']:
            text = extract_email_body(part)
            if text:
                return text
    else:
        mime_type = payload.get('mimeType', '')
        body_data = payload.get('body', {}).get('data', '')

        if body_data:
            decoded = base64.urlsafe_b64decode(body_data).decode('utf-8', errors='replace')
            if mime_type == 'text/plain':
                return decoded.strip()
            elif mime_type == 'text/html':
                soup = BeautifulSoup(decoded, 'html.parser') 
                return soup.get_text(separator='\n').strip()
    return ""

def normalize_text(s: str) -> str:
    if not s:
        return ''
    s = unicodedata.normalize('NFKC', s)
    nbsp_chars = ['\u00A0', '\u202F', '\u2007', '\u2060', '\uFEFF']
    for ch in nbsp_chars:
        s = s.replace(ch, ' ')
    s = re.sub(r'\s+', ' ', s).strip()
    return s    

def preprocess_time_edgecases(t):
    if not t:
        return t
    t = re.sub(r'(\d{1,2}(?::\d{2})?)([a-zA-Z]+)(?![ap]\.?m\.?)', r'\1', t, flags=re.IGNORECASE)
    t = re.sub(r'([a-zA-Z]+)(\d{1,2}(?::\d{2})?)(?![ap]\.?m\.?)', r'\2', t, flags=re.IGNORECASE)
    return t.strip()

def normalize_ampm(t):
    if not t:
        return t
    t = preprocess_time_edgecases(t)
    match = re.match(r'^\s*(\d{1,2})(?::(\d{2}))?\s*(?:[ap]\.?m\.?)?\s*$', t, flags=re.IGNORECASE)
    if not match:
        return t
    hour = int(match.group(1))
    minute = int(match.group(2)) if match.group(2) else 0
    if 7 <= hour <= 11:
        am_pm = "AM"
    elif hour == 12 or (1 <= hour <= 6):
        if hour == 6 and minute > 30:
            am_pm = "AM"
        else:
            am_pm = "PM"
    else:
        am_pm = "AM"
    if minute == 0:
        return f"{hour} {am_pm}"
    else:
        return f"{hour}:{minute:02d} {am_pm}"

def strip_forwarded_content(text: str) -> str:
    if not text:
        return text
    forwarded_markers = [
        "Forwarded message", "---------- Forwarded message ---------",
        "---------- Original Message ----------", "-------- Original message --------",
        "On Wed, ", "Begin forwarded message:"
    ]
    for marker in forwarded_markers:
        idx = text.lower().find(marker.lower()) 
        if idx != -1: 
            text = text[:idx] 
    return text.strip()

def contains_lesson_keywords(text):
    text = text.lower()
    return any(keyword in text for keyword in LESSON_KEYWORDS)    

def remove_month_dates(text: str) -> str:
    month_names = r"(January|Jan|February|Feb|March|Mar|April|Apr|May|June|Jun|July|Jul|" \
                  r"August|Aug|September|Sept|Sep|October|Oct|November|Nov|December|Dec)"
    month_day_pattern = rf"\b{month_names}\s+\d{{1,2}}\b"
    numeric_date_pattern = r"\b\d{1,2}/\d{1,2}\b"
    text = re.sub(month_day_pattern, "", text, flags=re.IGNORECASE)
    text = re.sub(numeric_date_pattern, "", text)
    return text.strip()

def parse_normalized_time(norm_time_str):
    if not norm_time_str:
        return None
    try:
        dt = datetime.strptime(norm_time_str.strip(), "%I:%M %p")
        return dt.time()
    except ValueError:
        pass
    try:
        dt = datetime.strptime(norm_time_str.strip(), "%I %p")
        return dt.time()
    except ValueError:
        return None

def parse_lesson_requests(subject: str, body: str, date_obj=None):
    day_map = {
        "mon": "Monday", "monday": "Monday",
        "tue": "Tuesday", "tues": "Tuesday", "tuesday": "Tuesday",
        "wed": "Wednesday", "weds": "Wednesday", "wednesday": "Wednesday",
        "thu": "Thursday", "thurs": "Thursday", "thursday": "Thursday",
        "fri": "Friday", "friday": "Friday",
        "sat": "Saturday", "saturday": "Saturday",
        "sun": "Sunday", "sunday": "Sunday"
    }
    day_regex = r'\b(Mon|Monday|Tue|Tues|Tuesday|Wed|Weds|Wednesday|Thu|Thurs|Thursday|Fri|Friday|Sat|Saturday|Sun|Sunday)\b'
    time_regex = r'(?:1[0-2]|0?\d)(?::[0-5]\d)?\s*(?:a\.?m\.?|p\.?m\.?)?'
    text = f"{subject} {body}"
    results = []
    stop_tokens = r"(?=\b" + day_regex + r"\b|Sent|Thanks|Racquets|Justin M. Foster|$)"
    pattern = re.compile(day_regex + r"\b(.*?)" + stop_tokens, re.IGNORECASE | re.DOTALL)
    seen_pairs = set()

    for match in pattern.finditer(text):
        raw_day = match.group(1) 
        day = day_map[raw_day.lower()]
        following_text = match.group(2) 
        start_idx = match.start()
        preceding_text = text[max(0, start_idx - 75):start_idx]
        times_after = re.findall(time_regex, following_text, re.IGNORECASE)
        times_before = re.findall(time_regex, preceding_text, re.IGNORECASE)
        unique_times = set(times_before + times_after) 
        for t in unique_times:
            norm_time = normalize_ampm(t)
            time_obj = parse_normalized_time(norm_time) 
            if (day, time_obj) not in seen_pairs:
                seen_pairs.add((day, time_obj))
                results.append({"day": day, "time": time_obj})
    
    days_found = re.findall(day_regex, text, re.IGNORECASE)
    times_found = re.findall(time_regex, text, re.IGNORECASE)
    if len(days_found) > 1 and len(times_found) == 1:
        normalized_days = [day_map[d.lower()] for d in days_found]
        norm_time = normalize_ampm(times_found[0])
        time_obj = parse_normalized_time(norm_time)
        for d in normalized_days:
            if (d, time_obj) not in seen_pairs:
                seen_pairs.add((d, time_obj))
                results.append({"day": d, "time": time_obj})

    if not results and days_found and not times_found:
        normalized_days = [day_map[d.lower()] for d in days_found]
        for d in normalized_days:
            if (d, None) not in seen_pairs:
                seen_pairs.add((d, None))
                results.append({"day": d, "time": None})

    if not results and date_obj:
        results.append({"day": date_obj.strftime('%A'), "time": None})

    return results

def format_time_for_output(t):
    if isinstance(t, time):
        return t.strftime("%I:%M %p").lstrip("0")
    return t

def main():
    service = authenticate_gmail()
    messages = fetch_emails(service)
    anchor_wed = most_recent_wednesday() 
    filtered_messages = filter_messages_by_anchor(messages, anchor_wed)

    lesson_requests = [] 
    processed_threads = set()
    
    for msg_detail in filtered_messages:
        thread_id = msg_detail['threadId']
        if thread_id in processed_threads:
            continue
        processed_threads.add(thread_id)
        thread = service.users().threads().get(userId='me', id=thread_id, format='full').execute()
        first_msg = thread['messages'][0]
        headers = first_msg['payload']['headers']
        subject = next((h['value'] for h in headers if h['name'] == 'Subject'), '')
        sender = next((h['value'] for h in headers if h['name'] == 'From'), '')
        date_str = next((h['value'] for h in headers if h['name'] == 'Date'), '')
        date_obj = parsedate_to_datetime(date_str) if date_str else None
        body = extract_email_body(first_msg['payload'])
        sender_email = sender.split('<')[-1].replace('>', '').strip().lower()

        if sender_email == MY_EMAIL.lower() or sender_email in BLOCKED_SENDERS:
            continue

        subject_clean = normalize_text(subject)
        body_clean = normalize_text(body)
        body_clean = strip_forwarded_content(body_clean)
        body_clean = remove_month_dates(body_clean)
        subject_clean = remove_month_dates(subject_clean)
        
        if contains_lesson_keywords(subject) or contains_lesson_keywords(body):
            display_name, _ = parseaddr(sender)
            player_name = display_name.strip() if display_name else 'Unknown'
            parsed_requests = parse_lesson_requests(subject_clean, body_clean, date_obj)
            for req in parsed_requests:
                lesson_requests.append({
                    'Date': req['day'],
                    'Player Name': player_name,
                    'Requested Time': req['time'],
                    'Subject': subject,
                    'Body Snippet': body_clean,
                    'Email': sender_email
                })

    if not lesson_requests:
        print("The lesson availability email hasn't been sent or no lessons requested yet")
        return

    df = pd.DataFrame(lesson_requests)
    weekday_order = {
        'Monday': 0, 'Tuesday': 1, 'Wednesday': 2, 'Thursday': 3,
        'Friday': 4, 'Saturday': 5, 'Sunday': 6
    }
    df['DayOrder'] = df['Date'].map(weekday_order)
    df['ParsedTime'] = df['Requested Time']
    df = df.sort_values(by=['DayOrder', 'ParsedTime'], na_position='last').drop(columns=['DayOrder', 'ParsedTime'])
    df['Requested Time'] = df['Requested Time'].apply(format_time_for_output)
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"Lesson requests saved to {OUTPUT_FILE}")

if __name__ == '__main__':
    main()
