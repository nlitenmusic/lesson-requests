import pandas as pd
# RS: pandas can read through different types of data filetypes and convert them into DataFrames (tables) or Series (columns) 
    # pandas is kind of like a Librarian sorting the 
    # NEW: it can also sort, filter and write CSVs 
        # takes all the lesson requests and organized them into a DataFrame 
            # --> sorts by date and time and player name
                # --> csv for future reference (nice and neat)
        # makes sense...a dataframe is basically a csv table already 

import os 
# provides functions for interacting with the operating system: file paths environment varibales, dictionary structure
# lets me find files no matter where the script runs: the name of my credential keys
    # its like the manager between me and the apps on my computer and the computers hardware
    # keeps track of files/folders so it knows where to save the lesson_requests.csv 
# RS: the os module is like a Python delivery system that can pick up information that lives on my computer so that python can safely access
    # then as I use that information in my codebase I can safely drop off new products in the their respective place on my computer 

from google.auth.transport.requests import Request
# RS: Request is in charge of the refresh tokens 
# If the previous token has expired Requests will then ask Gmail for a new token without having to log in again
    # like the API DMV checking to see if your ID has expired and creating new ID's if needed

from google_auth_oauthlib.flow import InstalledAppFlow
# RS: InstallAppFlow (class) like the RECEPTIONIST at the hotel who checks your ID and name, addesss before giving your temporary room key 
    # this opens a browser and lets you log in to your gmail and give your python script persmission to access gmail

from google.oauth2.credentials import Credentials
# this is a object that allows us access to our credentials
# If the client_secret and the token match our login we can put that information into a variable 'creds'
# Credentials are our VIP pass to get backstage (get the gmail API)

from googleapiclient.discovery import build
# The actual API part from what I think about when I got into APIs
# Once you have valid credentials, the build function creates a python object 'service" that lets you call gmail API method
    # eg. fetching messages or threads 
# RS: this is like having a gmail BUTLER who can provide whatever service I need based on the resources that I have access to

import json 
# JSON (JavaScript Object Notation) is used to store the information in objects so that you can later take them out of storage to use again
    # JSON is structured like a python dictionary 
    # eg. Google tokens are in JSON format
    # storing our token in JSON makes it so that we don't have to login in through the browser each time we want to access our API
    # like a KEYCHAIN 

import unicodedata
# used to normalize text so that my code can read it consistently 
# any time text is coming from an outside source its conventional to normalize 
# unicodedate is like a TRANSLATOR. Any quirks in twxt become normalized

import re
# RS: symbols that can be used to accurately parse data aka the Detective

import base64
# RS: gmail emails are typically wrapped in a secret code that needs to be deciphered so that Python can use it
    # base64 is the like Nicolas Cage for gmail base64 encryptions

from bs4 import BeautifulSoup
# Parsing HTML into plain text that you can actually parse
# It's like a VACUUM CLEANER sucking up the html tags leaving you with plain text (the clean floor) underneath

from email.utils import parsedate_to_datetime, parseaddr
# helpers for pulling useful information out of raw email headers which are messy
    # converting messy header strings into datetime objects (parsedate_to_datetime)
    # and clean name and email pair (parseaddr)
# these functions are like SORTERS that take only the information that you ask for from a messier mass of text

from datetime import datetime, timedelta, time

from dotenv import load_dotenv 

# ----------------------------
# CONFIG
# ----------------------------
load_dotenv()

SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
OUTPUT_FILE = os.getenv("OUTPUT_FILE")
MY_EMAIL = os.getenv("MY_EMAIL")
BLOCKED_SENDERS = os.getenv("BLOCKED_SENDERS").split(",")
CREDENTIALS_FILE = os.getenv("CREDENTIALS_FILE")
TOKEN_FILE = os.getenv("TOKEN_FILE")
script_dir = os.path.dirname(os.path.abspath(__file__))                                        

# Lesson-related keywords
LESSON_KEYWORDS = [
    'lesson', 'hi jordan', 'book', 'kg', 'tennis', 'schedule', 'wednesday'
]

# ----------------------------
# GMAIL AUTH 
# ----------------------------
def authenticate_gmail():
    creds = None 
    # initalizes credits no None each time 
    # eventually our Gmail API credentials go here

    credentials_file = os.path.join(script_dir, CREDENTIALS_FILE)
    # joins the client_secret with the folder into one comglomerate path that can be accesses from anywhere on my computer
    # returns a full path string to the json file

    # Token file path (stores the user's access and refresh tokens)
    token_path = os.path.join(script_dir, TOKEN_FILE)
    # returns a full path to the token fill which should live in the same folder
    # the first time this code was run there was no token.
    # this just tells python where the path should be like the address for a package

    # Try to load existing credentials safely
    if os.path.exists(token_path): 
    # checks if there is a path that leads to token_path (True or False)
    # if it exists we want to use it
        try:
        # try and except is a 'control-flow' tool 
        # try to do this risky thing and if it fails, handle the problem gracefully instead of crashing
            creds = Credentials.from_authorized_user_file(token_path, SCOPES)
            # tries to create an credentials object using class method that takes arguments for token path and Gmail SCOPES
            # either creates the instance or sends a Value Error
        except (json.JSONDecodeError, ValueError):
            # if an error happens, python jumps to the except block and instead of crashing runs the code below
            print("⚠️ token.json was invalid or empty. Deleting and starting fresh...")
            os.remove(token_path) 
            # removes the token file from my computer (thus os.remove) 
            creds = None
            # empties creds so that creds points to nothing

    # If credentials are missing or invalid, trigger login flow
    if not creds or not creds.valid:
    # if creds doesn't exists or is set to none or the credentials that we have live don't pass valid check 
    # valid is a boolean property which reads and checks the data
    # methods have parentheses which perform an action on the data
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        # this is going to check if the creditions exists, are expire and have a refresh token
            # if its true then we're going to refresh and call the Google server to refresh the token
        else:
            flow = InstalledAppFlow.from_client_secrets_file(credentials_file, SCOPES)
            # if no creds exist then we're going to create a flow object using the clients secret pathway 
            creds = flow.run_local_server(port=0)
            # takes the flow object and runs a method that opens in browser to manually verify your login
            # port 0 lets the system pick an open port for the local server
            # returns a credential after login 

        # Save the credentials for the next run
        with open(token_path, 'w') as token:
        # pythons 'context manager' syntax 
        # open() gives you a file object as TextIOWrapper, which gives you access to .read(), .write() methods
        # with is a cleaner way of opening a file and closing it once we're done instead of using close() at the end
        # with tells us to open and write the resulting token to the token_path
        # as token just assigns this value to a variable that we can immediately use to run methods in this block and no further
            token.write(creds.to_json())
            # with this token TextIOWrapper object representing our open token_path were going to convert the creds to a json and write that into the open token_path


    # Build Gmail API service
    service = build('gmail', 'v1', credentials=creds)
    # build is a function that creates a service object for the Gmail API 
    # argument list is long but the ones we use are (API, version, credentials=)
    return service
    # the purpose of our authenticate_gmail() function is to return a service we can use to butler emails lol


# ----------------------------
# FETCH EMAILS (MOST RECENT WEDNESDAY) 
# ----------------------------

def most_recent_wednesday(today=None):
    """Return the most recent Wednesday (including today if it's Wednesday)."""
    if today is None:
        today = datetime.today()
    days_since_wed = (today.weekday() - 2) % 7  # 2 = Wednesday
    return today - timedelta(days=days_since_wed)


def fetch_emails(service):
    """
    Fetch all Gmail messages from the most recent Wednesday through today.
    Returns a list of full message resources.
    """
    # Anchor Wednesday
    anchor_wed = most_recent_wednesday()

    # Adjust for Gmail's exclusive 'after:' logic
    after_date = (anchor_wed - timedelta(days=1)).strftime('%Y/%m/%d')  # include Wednesday
    before_date = (datetime.today() + timedelta(days=1)).strftime('%Y/%m/%d')  # include today

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
    """Keep only messages from anchor Wednesday through today."""
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




# ----------------------------
# BODY EXTRACTION: get clean searchable text from any Gmail messages so lesson request parser can do it's job
# ----------------------------
def extract_email_body(payload):
    # payload is a dictionary within the message resource
    """Return a clean text version of the email body, even if HTML-only."""
    # walks through gmails nested payload structure 
        # from messages().get(format='full') 
        # payload is a section of the message resource object returned 
    # payload is a python dictionary, not encrypted
    # it describes the headers, body and possible child parts
    # a payload is like a tree structure (MIME - Multipurpose Internet Mail Extension - tree)
        # carries different versions of content (plain text, HTML, attachments, images)
    if 'parts' in payload: # the parts drill/tester 
        # if there is text in the payload, there are no parts so we move on to our else statement
            # this part only handles multipart/alternative mime types 
        # parts are a list of dictionaries 
        # checks to see if the payload has parts 
        # body typically contains the email messages unless the email is multi-part (HTML, plain text, attachments
        # parts is an array that exists in multi-part emails
            # in which case 'parts' has the actually email message in one of its parts
        # if there are parts then its not a 'leaf node' (where the text lives)
        for part in payload['parts']: 
            # if parts exists we're going to dig into it up and iterate through each part
            # a recursive call treats every parts like it's a new payload
        # each part represents a different MIME type (text/plain, text/html, are the ones we're lookinf 
        # loop through the parts key to return part values 
            # print(part) {'partId': '0', 'mimeType': 'text/plain', 'filename': '', 'headers': [{'name': 'Content-Type', 'value': 'text/plain; charset="UTF-8"'}], 'body': {'size': 115, 'data': 'SGkgSm9yZGFuLA0KDQpEbyB5b3UgaGF2ZSBsZXNzb24gYXZhaWxhYmlsaXR5IG5leHQgTW9uZGF5LCBUdWVzZGF5LCBvciBXZWRuZXNkYXkgYXQgOWFtPw0KDQpUaGFua3MhDQoNCkJlc3QsDQpEYW4NCg=='}}
            text = extract_email_body(part)
            # NEW STACK FRAME CREATED (call it Frame 2) to run this function for new parts 
                # if the first part here is a leaf node (has a body with data) we hit the else statement and decode 
                # when that happens, text gets a truthy value
            # for each part that we find we're going to check if it has a parts list and open it 
                # if these parts don't have any more parts we go to our 'else' statement and decode
                # if they do then we'll go a layer deeper, opening up those parts and assigning them to our argument again
            # as we iterate through the parts we're going to assign them to a variable 'text'
            # loop through the parts key to return part values 
            # print(text) 
                # this prints out all of the email content that is 
            if text:
                # print(text)
                return text
            # returns send their values back up to their parents which also have returns so it bubbles up
            # such a neat trick! 

    else:
    # if there are no parts in the payload then the information we need to glean is either text/plain or text/html
        mime_type = payload.get('mimeType', '')
        # if there are no 'parts' in payload we'll check for the mimetype and take that as a variable
        # dict.get(key, default=None)
            # first parameter looks up the dictionary key,
            # second parameter says what to return if the key is missing

        body_data = payload.get('body', {}).get('data', '')
        # if there are no 'parts' in payload we'll check for the body data safely using .get()
        # body had a 2nd parameter default of { } because the value of the body is a dictionary 

        if body_data:
            decoded = base64.urlsafe_b64decode(body_data).decode('utf-8', errors='replace')
            # data in the body section is encoded so we need base64 to read the email message 
            # decodes the bytes into a UTF-8 string
            # if something is malformed we'll drop in a replacement character instead of crashing
            if mime_type == 'text/plain':
                return decoded.strip()
                # if the mimetype is plain text we'll decode and cut off the lead/trail whitespace of decoded body_data
                # these two return statements will return either to the new stack frame (if there were parts) 
                    # or directly to the parent function
                # as soon as its returned the function exits returning the value to its caller 

            elif mime_type == 'text/html':
                soup = BeautifulSoup(decoded, 'html.parser') 
                return soup.get_text(separator='\n').strip()
                # separator strips away tads and leaves just readable text, separating blocks with new lines
                # take the HTML body and turns it into just the lesson request text or, if forwarded, the outgoing email

    return ""
    # this will only run if the function reaches the bottom without hitting a return statement earlier

# ----------------------------
# TEXT NORMALIZATION 
# ----------------------------
# Make email bodies/subjects regex friendly 
def normalize_text(s: str) -> str:
    # s is just a parameter name (convention)
    # s represents the text that you're passing in (like the body or subject of an email)
    # : str means we expect it to be a string (for clarity)
    # -> str means this function will return a string (for clarity)
    # s should make you think "the text im cleaning"
    if not s:
        return ''
    # if s is Falsy then we'll just return an empty string 
    # prevents errors if your email doesn't have a subject 

    # Unicode compatibility normalization (e.g., fullwidth digits, odd punctuation)
    s = unicodedata.normalize('NFKC', s)
    # uses pythons unicodedata module to normalize text 
    # NFKC = Normalize Form KC (Compatibility Composition) 
        # converts 'equivalent but weird' characters to standard form
        # eg. 3 4 5 (fullwidth digits - used in Japanese text) to 345, fancy ligatures, curly quotes
        # makes all of the characters predictable 
        # s is the unicode string that we want to normalize, then we stick it back in s

    # create a list of common NBSP characters 
    nbsp_chars = [
        '\u00A0',  # Non-breaking space
        '\u202F',  # Narrow non-breaking space
        '\u2007',  # Figure space
        '\u2060',  # Word joiner
        '\uFEFF',  # Zero-width no-break space / BOM
    ]
    # NBSPs = Non-Breaking Space 
            # looks like a normal space, but it doesn't allow line breaks in text 
            # cannot wrap text 
    # \u00A0 is the most common NBSP
    # \u202F is a "narrow non-breaking space" often used in french text around : 

    # loop through the list to pull our each NBSP and replace with a normal space
    for ch in nbsp_chars:
        s = s.replace(ch, ' ') 
        
    # Collapse excess whitespace
    s = re.sub(r'\s+', ' ', s).strip()
    # we're going to substitute 
        # parameters (pattern, replace, string that we're pulling from)
    # then we're going to strip the white space (tabs, newlines, and any other white space)
    # the we strip down all of the trailing and leading whitespace so text has tidy edges

    return s    
    # the cleaned up text is returned

def preprocess_time_edgecases(t):
    """
    Remove any letters attached to numbers unless it's a valid AM/PM indicator.
    Handles:
        - Numbers stuck to letters at the end: '12noon' -> '12'
        - Letters stuck to numbers at the start: 'and11' -> '11'
        - Preserves 'am' or 'pm'
    """
    if not t:
        return t

    # Remove letters after a number if not am/pm
    t = re.sub(
        r'(\d{1,2}(?::\d{2})?)([a-zA-Z]+)(?![ap]\.?m\.?)',
        r'\1',
        t,
        flags=re.IGNORECASE
    )

    # Remove letters before a number if not am/pm
    t = re.sub(
        r'([a-zA-Z]+)(\d{1,2}(?::\d{2})?)(?![ap]\.?m\.?)',
        r'\2',
        t,
        flags=re.IGNORECASE
    )

    return t.strip()

# this functions job is to clean up time strings (2:30pm, 2 pm, 2 p.m. become 2:30 PM and 2 PM)
def normalize_ampm(t):
    """
    Normalize time strings using regex only:
        - All times 7:00–11:30 -> AM
        - All times 12:00–6:30 -> PM
        - Preprocess edge cases (12noon, 10and11, etc.)
    Returns a standardized string like "2 PM" or "2:30 PM".
    """
    if not t:
        return t

    # Preprocess edge cases first
    t = preprocess_time_edgecases(t)

    # Match possible formats: "2", "2:30", with or without am/pm
    match = re.match(r'^\s*(\d{1,2})(?::(\d{2}))?\s*(?:[ap]\.?m\.?)?\s*$', t, flags=re.IGNORECASE)
    if not match:
        return t  # can't normalize

    hour = int(match.group(1))
    minute = int(match.group(2)) if match.group(2) else 0

    # Decide AM/PM according to rules
    if 7 <= hour <= 11:
        am_pm = "AM"
    elif hour == 12 or (1 <= hour <= 6):
        # For hours 12–6 (and up to 6:30), assign PM
        if hour == 6 and minute > 30:
            am_pm = "AM"  # 6:31+ should not count as PM
        else:
            am_pm = "PM"
    else:
        # Fallback, assume AM
        am_pm = "AM"

    # Reconstruct string
    if minute == 0:
        return f"{hour} {am_pm}"
    else:
        return f"{hour}:{minute:02d} {am_pm}"
# ----------------------------
# STRIP FORWARDED CONTENT
# ----------------------------

def strip_forwarded_content(text: str) -> str:
    """
    Remove forwarded/quoted content so we don't parse lesson times
    from the club email blast or prior thread history.
    """
    if not text:
        return text 
        # doesn't try to do anything if there is falsy content in text

    # Cut off forwarded headers
    forwarded_markers = [
        "Forwarded message",
        "---------- Forwarded message ---------",
        "---------- Original Message ----------",
        "-------- Original message --------",
        "On Wed, ",
        "Begin forwarded message:"
    ]
    # list of strings that usually show up before forwarded message

    # common forwarding messages 
    for marker in forwarded_markers:
        idx = text.lower().find(marker.lower()) 
        # we're going to lowercase the text to make it easier to match
        # then iterate through the markers to find the first sighting of each of these marker options in the text
        # somehow that gives us an index unless it fails and the index is -1 
        # the text will end up being the body of our emails. we're looking for the markers inside of those emails
        # what were given back is the lowest index where those markers meet 
        if idx != -1: 
            text = text[:idx] 
            # if  it finds the marker in the text we will take the text from beginning until the index put that back into text
            # this omits everything after the first appearance of the marker
            # clever trick
    
    return text.strip()
    # mistake: when i deleted the quote function, i deleted the return so the function returned nothing

# ----------------------------
# DETECT LESSON REQUESTS
# ----------------------------
def contains_lesson_keywords(text):
    text = text.lower()
    return any(keyword in text for keyword in LESSON_KEYWORDS)    
    # for keyword in LESSON_KEYWORDS
        # is a 'generator expression' and it will loop through eack keyword in a list/set/etc 
        # 'in' evaluates wether each keyword is True or False 
    # keyword in text checks if the keyword is in the text...duh 
        # the we return True 
            # if at least one value in iterable is truthy
        # False
            # if they're all falsy

# ----------------------------
# REMOVE MONTH DATES
# ----------------------------

def remove_month_dates(text: str) -> str:
    """
    Remove month-related references (like 'August 25', 'Aug 25', '8/27')
    so they don't get misinterpreted as lesson times.
    """
    # Match full and abbreviated month names
    month_names = r"(January|Jan|February|Feb|March|Mar|April|Apr|May|June|Jun|July|Jul|" \
                  r"August|Aug|September|Sept|Sep|October|Oct|November|Nov|December|Dec)"

    # Pattern for "Month Day" (e.g., August 25, Aug 7)
    month_day_pattern = rf"\b{month_names}\s+\d{{1,2}}\b"

    # Pattern for numeric month/day (e.g., 8/27, 12/5)
    numeric_date_pattern = r"\b\d{1,2}/\d{1,2}\b"

    # Remove both kinds of matches
    text = re.sub(month_day_pattern, "", text, flags=re.IGNORECASE)
    text = re.sub(numeric_date_pattern, "", text)

    return text.strip()

# ----------------------------
# CONVERT TIME STRINGS TO DATE OBJ
# ----------------------------

def parse_normalized_time(norm_time_str):
    """
    Converts a normalized time string (e.g., "10 AM", "2:30 PM") to a datetime.time object.
    Returns None if parsing fails.
    """
    if not norm_time_str:
        return None
    
    # Attempt hour:minute with AM/PM
    try:
        dt = datetime.strptime(norm_time_str.strip(), "%I:%M %p")
        return dt.time()
    except ValueError:
        pass
    
    # Attempt hour only with AM/PM
    try:
        dt = datetime.strptime(norm_time_str.strip(), "%I %p")
        return dt.time()
    except ValueError:
        return None

# ----------------------------
# PARSE LESSON REQUESTS
# ----------------------------

def parse_lesson_requests(subject: str, body: str, date_obj=None):
    # the first two parameters are straight-forward the last will be a datetime object
    # what makes the date_obj an optional parameter is the 'None'
    # if no date_obj is assigned, None will be the default
    
    """
    Given an email subject/body, extract all requested lesson day/time pairs.
    Falls back to the email's Date header if no day is found.
    """

    day_map = {
        "mon": "Monday", "monday": "Monday",
        "tue": "Tuesday", "tues": "Tuesday", "tuesday": "Tuesday",
        "wed": "Wednesday", "weds": "Wednesday", "wednesday": "Wednesday",
        "thu": "Thursday", "thurs": "Thursday", "thursday": "Thursday",
        "fri": "Friday", "friday": "Friday",
        "sat": "Saturday", "saturday": "Saturday",
        "sun": "Sunday", "sunday": "Sunday"
    }
    # the keys are all of the words that we're going to convert to the cells in our Date column in our final output
    # normalizes shorthand into standard format

    day_regex = r'\b(Mon|Monday|Tue|Tues|Tuesday|Wed|Weds|Wednesday|Thu|Thurs|Thursday|Fri|Friday|Sat|Saturday|Sun|Sunday)\b'
    # when we need to use a regex pattern we have the day_regex variable preloaded 
    # the goal is to match all of these 
    # because of the \b these will read only on the other side of space or non-character symbol
    # capitalization doesn't matter since we will use re.IGNORECASE 
    # parentheses make this a capture group

    time_regex = r'(?:1[0-2]|0?\d)(?::[0-5]\d)?\s*(?:a\.?m\.?|p\.?m\.?)?'
    # \b matches the position between a non-letter character 
    # \s matches a whitespace character
    # ?: is a non-capture group
        # you can put parentheses around a series of non-capture groups and it will make it a capture group
        # even if all of the groups contained are non-capture you can make a capture by putting parentheses
    # * is 0 or more...
    # non-capture group for any 'at' before a whitespace character (optional)
    # then there is the main capture group with the hours (digits 10-12 and 01-09 optional 0)and minutes (optional) 

    text = f"{subject} {body}"
    # were going to name 'text' all of the string information that is in our subject line and body 
        # concatenate to one big searchable string
    # set up variable

    results = []
    # an empty list to collect all of out day/time pairs 
    # here we are going to store day/time dictionaries 

    stop_tokens = r"(?=\b" + day_regex + r"\b|Sent|Thanks|Racquets|$)"
    # creates a few options for where to stop reading the email so that phone numbers dont get consumed

    # Pass 1: Look for "Day ... time" patterns
    pattern = re.compile(day_regex + r"\b(.*?)" + stop_tokens, re.IGNORECASE | re.DOTALL)
    # ^ negates a class so it means match any non-letter character because it's in []
        # outside of a class ^ means start of the string (changed since)
    # re.compile() creates a pattern 
        # here we're concatenating our day_regex pattern with one or more of any non-letter character 
    # \b is a word boundary, which keeps the word whole and not capture the 'mon' inside of monster by accident
    # . matches any single character except a newline
    # .*? is a lazy capture capturing all of the characters up to a stop point 
        # what makes it lazy is that it stops at the first identified stop point
        # if it was just .* it would keep consuming matches if another iteration of the stop point exists
    # ?= looks ahead 
        # in this pattern it tells the lazy where the stop point is

    # re.DOTALL allows the . to match newlines if email text spans multiple lines 
        # basically a power up for the . regex to do it all

    seen_pairs = set() # initialize set to prevent duplicates of (day, time) pairs

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
    
    # Pass 2a: Multiple days, single time
    days_found = re.findall(day_regex, text, re.IGNORECASE)
    times_found = re.findall(time_regex, text, re.IGNORECASE)

    # Only proceed if more than one day is mentioned and exactly one time
    if len(days_found) > 1 and len(times_found) == 1:
        normalized_days = [day_map[d.lower()] for d in days_found]
        norm_time = normalize_ampm(times_found[0])
        time_obj = parse_normalized_time(norm_time)
        for d in normalized_days:
            if (d, time_obj) not in seen_pairs:
                seen_pairs.add((d, time_obj))
                results.append({"day": d, "time": time_obj})

    # Pass 2: Handle case where only days are found, but no times
    if not results:
        days_found = re.findall(day_regex, text, re.IGNORECASE)
        times_found = re.findall(time_regex, text, re.IGNORECASE)

        if days_found and not times_found:
            normalized_days = [day_map[d.lower()] for d in days_found]
            for d in normalized_days:
                if (d, None) not in seen_pairs:
                    seen_pairs.add((d, None))
                    results.append({"day": d, "time": None})

    # Pass 3: If still nothing, fallback to Date header
    if not results and date_obj:
        results.append({"day": date_obj.strftime('%A'), "time": None})

    return results

def format_time_for_output(t):
    if isinstance(t, time):
        return t.strftime("%I:%M %p").lstrip("0")
    return t

# ----------------------------
# MAIN SCRIPT
# ----------------------------
def main():
    # Authenticate with Gmail
    service = authenticate_gmail()

    # Fetch latest messages
    messages = fetch_emails(service)
    # print(messages[0])  {'id': '198d6ccc39e8d3a0', 'threadId': '198ce73afe0cd78a'}
    anchor_wed = most_recent_wednesday() 

    filtered_messages = filter_messages_by_anchor(messages, anchor_wed)

    lesson_requests = [] 
    processed_threads = set()  # keep track of threads we've already handled and make sure no duplicates are created
    
    for msg_detail in filtered_messages:
        thread_id = msg_detail['threadId']
        # print(thread_id) prints all 50 potential thread Ids and duplicates if there are multiple email Ids in the thread

        # Skip if we've already processed this thread
        if thread_id in processed_threads:
            continue
        processed_threads.add(thread_id)
        # adds email to processed threads. no affect if email is already in there
    
        # The first message in the thread
        thread = service.users().threads().get(userId='me', id=thread_id, format='full').execute()
        # print(thread) 
            # super detailed with information about sender dates, message its decoded already
        first_msg = thread['messages'][0]
        # print(first_msg) - prints every detail about the first message from the email thread
        
        # Parse sender, subject, body, date
        headers = first_msg['payload']['headers']
        # headers is a list of dictionaries (4) with each containing the name and 
        subject = next((h['value'] for h in headers if h['name'] == 'Subject'), '')
        # print(subject) # the literal subject lines 
        sender = next((h['value'] for h in headers if h['name'] == 'From'), '')
        # print(sender) Jordan Tolbert <jordan@belmonthillclub.net> <Natalie Mattessich <nataliemattessich@me.com>
        date_str = next((h['value'] for h in headers if h['name'] == 'Date'), '')
        # print(date_str) Tue, 26 Aug 2025 20:27:01 -0400 Tue, 26 Aug 2025 14:29:21 -0400# 
        date_obj = parsedate_to_datetime(date_str) if date_str else None
        # print(date_obj) 2025-08-26 14:29:21-04:00 2025-08-25 22:52:01-04:00
        body = extract_email_body(first_msg['payload'])
        # print(body) #prints entire email body for every email received using our recursive extract() function
        sender_email = sender.split('<')[-1].replace('>', '').strip().lower()
        # print(sender_email) jordan@belmonthillclub.net, mrcassi@gmail.com, belmonthill@playbypoint.com

        # Skip blocked or outgoing emails
        if sender_email == MY_EMAIL.lower() or sender_email in BLOCKED_SENDERS:
            continue

        # === CLEANING ===
        subject_clean = normalize_text(subject)
        body_clean = normalize_text(body)
        body_clean = strip_forwarded_content(body_clean)
        body_clean = remove_month_dates(body_clean)
        subject_clean = remove_month_dates(subject_clean)
        
        if contains_lesson_keywords(subject) or contains_lesson_keywords(body):
            display_name, _ = parseaddr(sender)
            # print(display_name)
            player_name = display_name.strip() if display_name else 'Unknown'
     
            # 🔑 Use helper to parse requests 
            parsed_requests = parse_lesson_requests(subject_clean, body_clean, date_obj)
            # parsed_requests gives us back a list of dictionaries containing {day:time} key value pairs
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

    # Save to CSV
    df = pd.DataFrame(lesson_requests)

    # Sort by day of the week (Monday → Sunday) 
    weekday_order = {
        'Monday': 0,
        'Tuesday': 1,
        'Wednesday': 2,
        'Thursday': 3,
        'Friday': 4,
        'Saturday': 5,
        'Sunday': 6
    }
    df['DayOrder'] = df['Date'].map(weekday_order)
    # we adding a new column to the lesson requests sheet that takes the day column and assigns it a value based on the weekday order dictionary
    # map() remaps each cell in a column using a dictionary or function and returns a .Series

    df['ParsedTime'] = df['Requested Time']
    # print(df['ParsedTime']) all None... so times are not properly converted to datetime obj

    # ✅ Correct column name: "Player Name" not "Player"
    df = df.sort_values(
        by=['DayOrder', 'ParsedTime'],
        na_position='last'
    ).drop(columns=['DayOrder', 'ParsedTime'])

    df['Requested Time'] = df['Requested Time'].apply(format_time_for_output)

    df.to_csv(OUTPUT_FILE, index=False)
    print(f"Lesson requests saved to {OUTPUT_FILE}")

if __name__ == '__main__':
    main()
