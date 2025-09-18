import pandas as pd
import os
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import json
import unicodedata
import re
import csv
import base64
from bs4 import BeautifulSoup
from email.utils import parsedate_to_datetime, parseaddr
from datetime import datetime, timedelta, time
from dotenv import load_dotenv

load_dotenv()

script_dir = os.path.dirname(os.path.abspath(__file__))
KNOWN_PLAYERS_PATH = os.environ.get("KNOWN_PLAYERS")
REGULAR_PLAYERS_PATH = os.environ.get("REGULAR_PLAYERS")
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
OUTPUT_FILE = os.getenv("OUTPUT_FILE")
MY_EMAIL = os.getenv("MY_EMAIL")
BLOCKED_SENDERS = os.getenv("BLOCKED_SENDERS").split(",")
CREDENTIALS_FILE = os.getenv("CREDENTIALS_FILE")
TOKEN_FILE = os.getenv("TOKEN_FILE")
CLUB_SCHEDULE = {
    "Monday": ["1:30pm", "2:30pm"],
    "Tuesday": ["2:30pm"],
    "Wednesday": ["7:00am", "9:00am", "10:00am", "2:30pm"],
    "Thursday": ["2:30pm"],
    "Sunday": ["12:00pm", "1:30pm", "2:30pm", "3:30pm"]
}
LESSON_KEYWORDS = [
    'lesson', 'hi jordan', 'book', 'kg' , 'schedule', 'private'
]

DAY_MAP = {
    "mon": "Monday", "monday": "Monday",
    "tue": "Tuesday", "tues": "Tuesday", "tuesday": "Tuesday",
    "wed": "Wednesday", "weds": "Wednesday", "wednesday": "Wednesday",
    "thu": "Thursday", "thurs": "Thursday", "thursday": "Thursday",
    "fri": "Friday", "friday": "Friday",
    "sat": "Saturday", "saturday": "Saturday",
    "sun": "Sunday", "sunday": "Sunday"
}

NUM_MAP = {
        "one": "1",
        "two": "2",
        "three": "3",
        "four": "4",
        "five": "5",
        "six": "6",
        "seven": "7",
        "eight": "8",
        "nine": "9",
        "ten": "10",
        "eleven": "11",
        "twelve": "12",
        "noon": "12:00pm",
        "midnight": "12:00am"
    }

DAY_REGEX = r'\b(Mon|Monday|Tue|Tues|Tuesday|Wed|Weds|Wednesday|' \
            r'Thu|Thurs|Thursday|Fri|Friday|Sat|Saturday|Sun|Sunday)\b'

TIME_REGEX = r'(?:1[0-2]|0?\d)(?::[0-5]\d)?\s*(?:a\.?m\.?|p\.?m\.?)?'


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

def normalize_dotted_times(text: str) -> str:
    """
    Convert dotted times like '2.30' or '10.45am' into '2:30' / '10:45am'
    so they can be parsed by the normal pipeline.
    """
    import re
    # Match 1–2 digit hour, dot, 2 digit minutes, optional am/pm
    return re.sub(
        r"\b(\d{1,2})\.(\d{2})(\s*[ap]\.?m\.?)?\b",
        lambda m: f"{m.group(1)}:{m.group(2)}{m.group(3) or ''}",
        text,
        flags=re.IGNORECASE,
    )

def normalize_text(s: str) -> str:
    if not s:
        return ''
    s = unicodedata.normalize('NFKC', s)
    nbsp_chars = ['\u00A0', '\u202F', '\u2007', '\u2060', '\uFEFF']
    for ch in nbsp_chars:
        s = s.replace(ch, ' ')
    s = re.sub(r'\s+', ' ', s).strip()
    return s    

def skip_one_hour_phrases(text: str) -> str:
    """
    Removes 'one hour' / 'one-hour' / 'one–hour' / 'one—hour' and
    other dash variants so they don't parse as 1pm.
    """
    import re
    # \u2010 to \u2015 = common Unicode dash characters
    # \u2212 = minus sign (sometimes shows up)
    dash_chars = r"\-\u2010-\u2015\u2212"
    pattern = rf"\bone[\s{dash_chars}]?hour\b"
    return re.sub(pattern, "", text, flags=re.IGNORECASE)

import re

def remove_ordinals(text: str) -> str:
    """
    Removes numbers that have ordinal suffixes (1st, 2nd, 3rd, 24th, etc.)
    so they aren’t misinterpreted as lesson times.
    """
    # Match digits followed by st, nd, rd, th (case-insensitive)
    return re.sub(r'\b\d+(?:st|nd|rd|th)\b', '', text, flags=re.IGNORECASE)


def replace_written_times(text: str) -> str:
    """
    Replace written-out times like 'one', 'two thirty', 'noon', 'midnight'
    with numeric clock times (e.g., '1:00', '2:30', '12:00pm').
    Ignores cases like 'one of', 'two of'.
    """
    # Basic map of numbers to digits

    # Handle special tokens first
    text = re.sub(r"\bnoon\b", "12:00pm", text, flags=re.IGNORECASE)
    text = re.sub(r"\bmidnight\b", "12:00am", text, flags=re.IGNORECASE)

    # Match patterns like "two thirty", "seven fifteen", "nine o'clock"
    # Normalize to digit + :minutes
    def replace_match(m):
        hour_word = m.group(1).lower()
        minute_word = m.group(2)
        hour_num = NUM_MAP.get(hour_word, hour_word)

        if minute_word:
            minute_word = minute_word.lower()
            if minute_word in ["thirty"]:  # extendable
                minute_num = "30"
            elif minute_word in ["fifteen", "quarter"]:
                minute_num = "15"
            elif minute_word in ["fortyfive", "forty-five", "three-quarters"]:
                minute_num = "45"
            elif minute_word in ["o'clock"]:
                minute_num = "00"
            else:
                return m.group(0)  # leave unchanged if unknown
            return f"{hour_num}:{minute_num}"
        else:
            return f"{hour_num}:00"

    # Add negative lookahead (?!\s+of) so "one of" / "two of" are ignored
    time_pattern = re.compile(
        r"\b(" + "|".join(NUM_MAP.keys()) + r")\b(?!\s+of)"
        r"\s*(thirty|fifteen|quarter|fortyfive|forty-five|three-quarters|o'clock)?\b",
        flags=re.IGNORECASE,
    )
    text = time_pattern.sub(replace_match, text)

    return text

def expand_afternoon_text(text, club_schedule):
    """
    Replace occurrences like 'Sunday afternoon' with explicit '<Day> <Time>' strings
    for each club slot after 12pm.
    """
    import re
    import datetime

    day_regex = r"\b(Mon|Monday|Tue|Tues|Tuesday|Wed|Weds|Wednesday|Thu|Thurs|Thursday|Fri|Friday|Sat|Saturday|Sun|Sunday)\b"

    def replace_afternoon(match):
        day_word = match.group(1)
        day = DAY_MAP[day_word.lower()]
        times = club_schedule.get(day, [])

        afternoon_times = []
        for t in times:
            try:
                dt_obj = parse_normalized_time(normalize_ampm(t))
                if dt_obj and dt_obj >= datetime.time(12, 0):
                    # Prepend day to ensure correct pairing in parser
                    afternoon_times.append(f"{day} {t}")
            except Exception:
                continue  # skip any times that fail parsing

        return " ".join(afternoon_times)

    return re.sub(day_regex + r"\s+afternoon", replace_afternoon, text, flags=re.IGNORECASE)


def remove_excluded_times(text: str) -> str:
    """
    Remove time mentions that follow exclusion phrases such as:
    'except', 'but not', 'other than', 'not'.
    Example: 'except 7 am', 'but not 12', 'other than 2:30'.
    """
    exclusion_patterns = [
        r"(?:except|but not|other than|not)\s+((?:\d{1,2}(?::\d{2})?\s*(?:am|pm)?)|\b(?:one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|noon|midnight)\b)",
    ]

    cleaned_text = text
    for pat in exclusion_patterns:
        cleaned_text = re.sub(pat, "", cleaned_text, flags=re.IGNORECASE)

    return cleaned_text


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

def strip_end_content(text: str) -> str:
    if not text:
        return text
    forwarded_markers = [
        "Forwarded message", "---------- Forwarded message ---------",
        "---------- Original Message ----------", "-------- Original message --------",
        "On Wed, ", "Begin forwarded message:", "Thanks", "Best", "---------------------"
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

def strip_signature_numbers(text: str) -> str:
    if not text:
        return text

    patterns = [
        # Phone numbers
        r"(?:\+1\s*[-.\s]?)?(?:\(?\d{3}\)?[-.\s]?)\d{3}[-.\s]?\d{4}(?:\s*(?:ext\.?|x|extension)\s*\d+)?",
        # Zip codes (5 or 9 digit)
        r"\b\d{5}(?:-\d{4})?\b",
        # Street numbers (at start of a line or before a street name)
        r"\b\d{1,5}\s+(?:[A-Za-z]+\s){0,3}(?:Street|St|Avenue|Ave|Road|Rd|Lane|Ln|Boulevard|Blvd|Court|Ct)\b",
    ]

    for pat in patterns:
        text = re.sub(pat, "", text, flags=re.IGNORECASE)

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

def preprocess_club_schedule(schedule):
    """Take raw CLUB_SCHEDULE times and convert them into datetime.time objects."""
    processed = {}
    for day, times in schedule.items():
        processed_times = []
        for t in times:
            # Step 1: normalize into AM/PM string
            norm_str = normalize_ampm(t)

            # Step 2: turn that normalized string into a datetime.time object
            dt_time = parse_normalized_time(norm_str)

            if dt_time:
                processed_times.append(dt_time)

        processed[day] = processed_times
    return processed

def expand_lesson_requests(requests, club_schedule):
    expanded_requests = []
    for req in requests:
        day = req["Date"]
        requested_time = req.get("Requested Time")

        if requested_time:
            expanded_requests.append(req.copy())
        else:
            available_times = club_schedule.get(day, [])
            for t in available_times:  # t is already a datetime.time object
                expanded_req = req.copy()
                expanded_req["Requested Time"] = t
                expanded_requests.append(expanded_req)

    return expanded_requests

def parse_subject_only(subject: str):
    """
    Parse lesson requests from the email subject only. 
    Handles multiple days and multiple times, normalized to datetime.time objects.
    """
    results = []
    seen_pairs = set()

    # Find all day references in the subject
    days_found = re.findall(DAY_REGEX, subject, re.IGNORECASE)

    # Find all times in the subject
    times_found = re.findall(TIME_REGEX, subject, re.IGNORECASE)

    # Normalize times
    time_objs = [parse_normalized_time(normalize_ampm(t)) for t in times_found]

    # Map each day to all requested times
    for d in days_found:
        day = DAY_MAP[d.lower()]
        if time_objs:
            for time_obj in time_objs:
                if (day, time_obj) not in seen_pairs:
                    seen_pairs.add((day, time_obj))
                    results.append({"day": day, "time": time_obj})
        else:
            # If no times found, still create entry with None
            if (day, None) not in seen_pairs:
                seen_pairs.add((day, None))
                results.append({"day": day, "time": None})

    return results    

def parse_lesson_requests(subject: str, body: str, player_name, known_players, date_obj=None, sender_email=None):
    text = f"{subject} {body}"
    text = skip_one_hour_phrases(text)
    text = remove_ordinals(text)
    text = remove_excluded_times(text)
    text = normalize_dotted_times(text) 
    text = expand_afternoon_text(text, CLUB_SCHEDULE)
    text = replace_written_times(text)
    results = []
    stop_tokens = r"(?=" + DAY_REGEX + r"|Sent|Thanks|Racquets|$)"
    pattern = re.compile(DAY_REGEX + r"\b(.*?)" + stop_tokens, re.IGNORECASE | re.DOTALL)
    seen_pairs = set()

    # Primary pass: find explicit day mentions and associated times (prefer times after the day)
    for match in pattern.finditer(text):
        raw_day = match.group(1)
        day = DAY_MAP[raw_day.lower()]
        following_text = match.group(2)
        start_idx = match.start()

        times_after = re.findall(TIME_REGEX, following_text, re.IGNORECASE)
        if times_after:
            unique_times = set(times_after)
        else:
            preceding_text = text[max(0, start_idx - 30):start_idx]  # tighter window
            times_before = re.findall(TIME_REGEX, preceding_text, re.IGNORECASE)
            unique_times = set(times_before)

        for t in unique_times:
            norm_time = normalize_ampm(t)
            time_obj = parse_normalized_time(norm_time)
            if (day, time_obj) not in seen_pairs:
                seen_pairs.add((day, time_obj))
                results.append({"day": day, "time": time_obj})

    # Post-pass: collect all found day/time tokens for other heuristics
    days_found = re.findall(DAY_REGEX, text, re.IGNORECASE)
    times_found = re.findall(TIME_REGEX, text, re.IGNORECASE)

    # Case: multiple days listed and a single time -> map that time to all days
    if len(days_found) > 1 and len(times_found) == 1:
        normalized_days = [DAY_MAP[d.lower()] for d in days_found]
        norm_time = normalize_ampm(times_found[0])
        time_obj = parse_normalized_time(norm_time)
        for d in normalized_days:
            if (d, time_obj) not in seen_pairs:
                seen_pairs.add((d, time_obj))
                results.append({"day": d, "time": time_obj})

    # Case: days found but no times -> create day entries with time=None
    if not results and days_found and not times_found:
        normalized_days = [DAY_MAP[d.lower()] for d in days_found]
        for d in normalized_days:
            if (d, None) not in seen_pairs:
                seen_pairs.add((d, None))
                results.append({"day": d, "time": None})

    # NEW: times found but no days -> match the sender by email and use their usual_slots
    if not results and times_found and not days_found and sender_email:
        norm_times = [normalize_ampm(t) for t in times_found]
        time_objs = [parse_normalized_time(nt) for nt in norm_times if parse_normalized_time(nt) is not None]
        if time_objs:
            known_player = find_known_player(known_players, sender_email=sender_email, player_name=player_name)
            if known_player:
                usual_slots_str = known_player.get("usual_slots", "")
                if usual_slots_str:
                    tokens = re.split(r"[,\s]+", usual_slots_str.strip())
                    for token in tokens:
                        if not token:
                            continue
                        token_day = DAY_MAP.get(token.lower())
                        if not token_day:
                            continue
                        for time_obj in time_objs:
                            if (token_day, time_obj) not in seen_pairs:
                                seen_pairs.add((token_day, time_obj))
                                results.append({"day": token_day, "time": time_obj})

    # FINAL fallback: no parsed results at all -> try known_player usual_slots -> date_obj -> None
    if not results:
        known_player = find_known_player(known_players, sender_email=sender_email, player_name=player_name)
        if known_player:
            usual_slots_str = known_player.get("usual_slots", "")
            if usual_slots_str:
                tokens = re.split(r"[,\s]+", usual_slots_str.strip())
                for token in tokens:
                    if not token:
                        continue
                    token_day = DAY_MAP.get(token.lower())
                    if not token_day:
                        continue
                    if (token_day, None) not in seen_pairs:
                        seen_pairs.add((token_day, None))
                        results.append({"day": token_day, "time": None})
                # done with known_player fallback
            elif date_obj:
                day = date_obj.strftime('%A')
                results.append({"day": day, "time": None})
            else:
                results.append({"day": None, "time": None})
        elif date_obj:
            # No known player record, but we have an email date to fall back on
            day = date_obj.strftime('%A')
            results.append({"day": day, "time": None})
        else:
            results.append({"day": None, "time": None})

    return results

def format_time_for_output(t):
    if isinstance(t, time):
        return t.strftime("%I:%M %p").lstrip("0")
    return t

def load_known_players(filepath):
    if not os.path.exists(filepath):
        print(f"⚠️ Known players file not found at {filepath}")
        return []

    known_players = []
    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            kp = {
                "Player Name": row.get("name", "").strip(),
                "Email": row.get("email", "").strip().lower(),
                # tolerant mapping: prefer explicit 'usual_slots' but accept other column names
                "usual_slots": (row.get("usual_slots") or row.get("usual_date") or
                                row.get("usual_time") or row.get("Date") or "").strip()
            }
            known_players.append(kp)
    return known_players

def load_regular_players(filepath):
    if not os.path.exists(filepath):
        print(f"⚠️ Regular players file not found at {filepath}")
        return []
    regular_players = []
    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            regular_players.append({
                "name": row["name"].strip(),
                "email": row["email"].strip(),
                "usual_slots": row["usual_date"].strip()
            })
    return regular_players

def find_known_player(known_players, sender_email=None, player_name=None):
    if sender_email:
        kp = next((kp for kp in known_players if kp.get("Email","").lower() == sender_email.lower()), None)
        if kp:
            return kp
    if player_name:
        kp = next((kp for kp in known_players if kp.get("Player Name","").lower() == player_name.lower()), None)
        return kp
    return None

def expand_regular_players(regular_players, processed_schedule):
    """
    Expand regular players into lesson request entries
    using the preprocessed club schedule for consistency.
    """
    results = []
    for player in regular_players:
        days = [d.strip() for d in re.split(r'[,\s]+', player["usual_slots"]) if d.strip()]
        for day in days:
            if day not in processed_schedule:
                continue  # skip invalid days
            for slot in processed_schedule[day]:
                results.append({
                    "Player Name": player["name"],
                    "Email": player["email"],
                    "Date": day,
                    "Requested Time": slot,
                })
    return results

def main():
    known_players = load_known_players(KNOWN_PLAYERS_PATH)
    service = authenticate_gmail()
    messages = fetch_emails(service)
    anchor_wed = most_recent_wednesday() 
    filtered_messages = filter_messages_by_anchor(messages, anchor_wed)

    lesson_requests = [] 
    processed_threads = set()
    keyword_threads = set()           # threads matched by keyword parser
    subject_only_candidates = []
    
    # --- first pass: keyword parsing (populate lesson_requests) and collect candidates ---
    for msg_detail in filtered_messages:
        thread_id = msg_detail['threadId']
        if thread_id in processed_threads:
            continue
        processed_threads.add(thread_id)

        # fetch the full thread
        thread = service.users().threads().get(userId='me', id=thread_id, format='full').execute()
        first_msg = thread['messages'][0]
        headers = first_msg['payload'].get('headers', [])
        subject = next((h['value'] for h in headers if h['name'] == 'Subject'), '')
        sender = next((h['value'] for h in headers if h['name'] == 'From'), '')
        date_str = next((h['value'] for h in headers if h['name'] == 'Date'), '')
        date_obj = parsedate_to_datetime(date_str) if date_str else None
        body = extract_email_body(first_msg['payload'])
        sender_email = sender.split('<')[-1].replace('>', '').strip().lower()

        # ignore self or blocked senders
        if sender_email == MY_EMAIL.lower() or sender_email in BLOCKED_SENDERS:
            continue

        # clean text
        subject_clean = normalize_text(subject)
        body_clean = normalize_text(body)
        body_clean = strip_end_content(body_clean)
        body_clean = remove_month_dates(body_clean)
        body_clean = strip_signature_numbers(body_clean)
        subject_clean = remove_month_dates(subject_clean)

        # who sent it (for naming)
        display_name, _ = parseaddr(sender)
        player_name = display_name.strip() if display_name else 'Unknown'

        # If email contains lesson keywords -> use main parser and append results
        if contains_lesson_keywords(subject) or contains_lesson_keywords(body):
            parsed_requests = parse_lesson_requests(
                subject_clean,
                body_clean,
                player_name,
                known_players,
                date_obj=date_obj,
                sender_email=sender_email
            )
            if parsed_requests:
                for req in parsed_requests:
                    lesson_requests.append({
                        'Date': req['day'],
                        'Player Name': player_name,
                        'Requested Time': req['time'],
                        'Subject': subject,
                        'Body Snippet': body_clean,
                        'Email': sender_email
                    })
                keyword_threads.add(thread_id)   # mark this thread as handled by keywords
            # if parsed_requests empty, we still don't put this thread through subject-only
            # because it had lesson keywords (explicit request) and you said first-pass should own that.
        else:
            # Candidate for subject-only fallback: store minimal handy pieces for second pass
            subject_only_candidates.append({
                'thread_id': thread_id,
                'subject_clean': subject_clean,
                'subject_raw': subject,
                'body_snippet': body_clean,
                'sender_email': sender_email,
                'player_name': player_name,
                'date_obj': date_obj
            })

    # --- second pass: subject-only parsing for threads not matched by keywords ---
    for c in subject_only_candidates:
        # skip if the same thread somehow got handled by the keyword pass
        if c['thread_id'] in keyword_threads:
            continue

        parsed = parse_subject_only(c['subject_clean'])
        if not parsed:
            continue

        # attach player info and append to the same lesson_requests list
        for req in parsed:
            lesson_requests.append({
                'Date': req['day'],
                'Player Name': c['player_name'],
                'Requested Time': req['time'],
                'Subject': c['subject_raw'],
                'Body Snippet': c['body_snippet'],
                'Email': c['sender_email']
            })

    if not lesson_requests:
        print("The lesson availability email hasn't been sent or no lessons requested yet")
        return
    
    processed_schedule = preprocess_club_schedule(CLUB_SCHEDULE)
    lesson_requests = expand_lesson_requests(lesson_requests, processed_schedule)

    regular_players = load_regular_players(REGULAR_PLAYERS_PATH)
    regular_entries = expand_regular_players(regular_players, processed_schedule)
    lesson_requests.extend(regular_entries)

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
