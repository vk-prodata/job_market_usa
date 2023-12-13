from ast import List, Tuple
import datetime
from math import inf
import re
import numpy as np
import pandas as pd

from pyparsing import Optional


def extract_salaries(text: str):
    """
    Extracts all dollar amounts (with or without cents) from a given text string and returns them as a list of integers.

    Args:
        text (str): A text string that may contain salary information in the form of dollar amounts with or without cents,
                    e.g. "$10,000" or "$10,000.00".

    Returns:
        List[int]: A list of integer values representing the extracted salaries from the input text. Each integer
                   represents the salary in dollars and cents, with cents rounded down to the nearest dollar
                   (e.g. $10,000.99 would be represented as 10000). If no salaries are found in the input text,
                   an empty list is returned.
    """
    if not text: return None

    # Define a regular expression pattern to match dollar amounts
    pattern = r"\$\s?\d{1,3}(?:[,.]\d{3})*(?:\.\d{2})?"

    # Find all matches in the text and return as a list
    salaries = re.findall(pattern, str(text))

    # Convert all salaries to the same format
    for i in range(len(salaries)):
        salaries[i] = int(float(salaries[i].replace('.', '').replace(',', '').replace('$', '')))
    return salaries


def extract_salaries_str(text: str):
    """
    Extracts all dollar amounts (with or without cents) from a given text string and returns them as a list of integers.

    Args:
        text (str): A text string that may contain salary information in the form of dollar amounts with or without cents,
                    e.g. "$10,000" or "$10,000.00".

    Returns:
        List[str]: A list of string values representing the extracted salaries from the input text.
    """
    # Define a regular expression pattern to match dollar amounts
    pattern = r"\$\d{1,3}(?:,\d{3})*(?:\.\d{2})?"

    # Find all matches in the text and return as a list
    salaries = re.findall(pattern, text)

    # Convert all salaries to the same format
    for i in range(len(salaries)):
        salaries[i] = salaries[i].replace(',', '')  # Remove commas
        if '.' not in salaries[i]:
            salaries[i] += '.00'  # Add .00 if no decimal point

    return salaries

def average_salary(text: str) -> float:
    salaries = extract_salaries(text)
    if len(salaries) == 1:
        return align_salary(salaries[0], text)
    elif len(salaries) > 1:
        return align_salary((salaries[0] + salaries[1])/ 2, text)
    else:
        return None

def align_salary(salary:str,text: str):
    res = None
    s = str(salary)
    if text.find('hr') >= 0: #if salary in hour
        if salary > 100:
            res = float(s[0:2]) * 2080 / 1000 #2080 hour avg in a year
        else:
            res = salary * 2080 / 1000
    else:
        if 40 < salary < 900:
            res = salary

    return res

def convert_date_to_week_start(datestr):
    # Convert string to datetime object
    date = datetime.datetime.strptime(datestr, '%Y-%m-%d %H:%M:%S.%f')
    
    # Subtract the current day of the week from the date to get to the start of the week (Monday)
    start_of_week = date - datetime.timedelta(days=date.weekday())
    
    # Return the date part only
    return start_of_week.date().strftime('%Y-%m-%d')
    # date = datetime.datetime.strptime(datestr, '%d-%m-%Y').date()
    # start_of_week = date - datetime.timedelta(days=date.weekday())
    # return start_of_week.strftime('%Y-%m-%d')

def extract_date_from_filename(filename):
    match = re.search(r'\d{2}-\d{2}-\d{4}', filename)
    if match:
        return convert_date_to_week_start(match.group(0))
    else:
        return None
    
#deprecated
def fill_date_posting(date_posting):
    if pd.isna(date_posting):
        return (datetime.datetime.now() - datetime.timedelta(days=7)).strftime("%m/%d/%Y")

    return date_posting

def extract_seniority(title: str) -> str:
    seniority_keywords = {"senior":"Senior", "sr":"Senior", "lead":"Senior", "staff":"Staff/Lead", "lead":"Staff/Lead", "vp":" VP/Principal", "principal":" VP/Principal", "director":" VP/Principal", "junior":"Jr/Intern", "jr":"Jr/Intern", "intern":"Jr/Intern"}
    for keyword, sen in seniority_keywords.items():
        if keyword in title.lower():
            return sen

    return None

def remote_type(location, title: str) -> str:
    type_keywords = {"remote":"Remote", "hybrid":"Hybrid", "onsite":"Onsite", "on-site":"Onsite"}
    for keyword, sen in type_keywords.items():
        if keyword in location.lower():
            return sen
        if keyword in title.lower():
            return sen
    return None

def cloud_type(title: str) -> str:
    type_keywords = {"aws":"AWS", "gcp":"GCP", "azure":"Azure", "google":"Google", "cloud":"Any Cloud"}
    for keyword, sen in type_keywords.items():
        if keyword in title.lower():
            return sen
    return None

def extract_positition_type(title: str) -> str:
    type_keywords = {"machine learning": "Machine Learning", "ml ": "Machine Learning",  "analyst":"Data Analyst", "analysis":"Data Analyst", "science":"Data Science", "analytics":"Analytics Engineer", "intelligence":"BI Developer", "engineer":"Data Engineer", "platform":"Platform Engineer", "data": "Any Data", "bi":"BI Developer",  "software":"Software Engineer"}
    for keyword, sen in type_keywords.items():
        if keyword in title.lower():
            return sen

    return "Other"


def remove_extra_info_from_title(text):
    # Find the index of the first occurrence of a comma, digit, or bracket in the string
    index = len(text)
    for char in [',', '(', ')', '[', ']', '{', '}','|','!','/', '-','0', '1', '2', '3', '4', '5', '6', '7', '8', '9']:
        if char in text:
            char_index = text.index(char)
            if char_index < index and char_index > 10: # clean word shouldn't be too small
                index = char_index
    # Remove everything after the index and append the modified string to the new list
    return text[:index].strip()



def shorten_state(state: str) -> str:
    states = {'alabama': 'AL', 'alaska': 'AK', 'arizona': 'AZ', 'arkansas': 'AR', 'california': 'CA',
              'colorado': 'CO', 'connecticut': 'CT', 'delaware': 'DE', 'dc':'DC', 'florida': 'FL', 'georgia': 'GA',
              'hawaii': 'HI', 'idaho': 'ID', 'illinois': 'IL', 'indiana': 'IN', 'iowa': 'IA',
              'kansas': 'KS', 'kentucky': 'KY', 'louisiana': 'LA', 'maine': 'ME', 'maryland': 'MD',
              'massachusetts': 'MA', 'michigan': 'MI', 'minnesota': 'MN', 'mississippi': 'MS',
              'missouri': 'MO', 'montana': 'MT', 'nebraska': 'NE', 'nevada': 'NV', 'new hampshire': 'NH',
              'new jersey': 'NJ', 'new mexico': 'NM', 'new york': 'NY', 'north carolina': 'NC',
              'north dakota': 'ND', 'ohio': 'OH', 'oklahoma': 'OK', 'oregon': 'OR', 'pennsylvania': 'PA',
              'rhode island': 'RI', 'south carolina': 'SC', 'south dakota': 'SD', 'tennessee': 'TN',
              'texas': 'TX', 'utah': 'UT', 'vermont': 'VT', 'virginia': 'VA', 'washington': 'WA',
              'west virginia': 'WV', 'wisconsin': 'WI', 'wyoming': 'WY'}

    if len(state) == 2 and state.upper() in states.values():
        return state.upper()

    state = state.lower()
    return states.get(state, "USA")

def clean_location(location:str) -> str:
    return location.replace('Greater', '').replace('Metropolitan', '').replace('County', '').replace('Metro', '').replace('Area','').replace('(Remote)','').replace('(Onsite)','').replace('(Hybrid)','').replace('(On-site)','').strip()

def split_location(location: str):
    """
    Splits a location string into its city and state components.

    Args:
        location (str): A string representing a location in the format "city, state" or "city". The state component
                         is optional.

    Returns:
        Tuple[str, Optional[str]]: A tuple containing the city and state components of the input location string. If
                                   the input location string does not contain a state component, the second element of
                                   the tuple will be None.
    """
    #result = Tuple[str, Optional[str]]
    city_state = clean_location(location).split(", ")
    if location.endswith('US') or location.find('United States') >= 0:
        #city_state = city_state.split(" ")
        if len(city_state) == 1:
            result = None, "USA"
        elif len(city_state) == 2:
            result = None, shorten_state(city_state[0])
        elif len(city_state) == 3:
            result = city_state[0], shorten_state(city_state[1])
        else:
            result = city_state[0], city_state[1].split(" ")[0]
    else:
        if len(city_state) == 1:
            if  shorten_state(city_state[0]) ==  "USA":
                result = city_state[0],  "USA"
            else:
                result = None, shorten_state(city_state[0])
        elif len(city_state) >= 2:
            state = city_state[1] #.split(" ")
            result =city_state[0], shorten_state(state)
        else:
            result = None, "USA"
    return result


def coalesce(*values):
    """Return the first non-None value or None if all values are None"""
    return next((v for v in values if v is not None), None)

def extract_location_from_filename(filename):
    parts = filename.split("_")
    location = parts[2]
    return location