from ast import List, Tuple
import re

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
        return salaries[0]
    elif len(salaries) > 1:
        return (salaries[0] + salaries[1])/ 2
    else:
        return None

def shorten_state(state: str) -> str:
    states = {'alabama': 'AL', 'alaska': 'AK', 'arizona': 'AZ', 'arkansas': 'AR', 'california': 'CA',
              'colorado': 'CO', 'connecticut': 'CT', 'delaware': 'DE', 'florida': 'FL', 'georgia': 'GA',
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
    return states.get(state, "Anywhere US")

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
    city_state = location.split(", ")
    if location.endswith('US') or location.endswith('United States'):
        if len(city_state) == 1:
            result = None, "Anywhere US"
        elif len(city_state) == 2:
            result = None, shorten_state(city_state[0])
        elif len(city_state) == 3:
            result = city_state[0], shorten_state(city_state[1])
        else:
            result = city_state[0], city_state[1].split(" ")[0]
    else:
        if len(city_state) == 1:
            if  shorten_state(city_state[0]) ==  "Anywhere US":
                result = city_state[0],  "Anywhere US"
            else:
                result = None, shorten_state(city_state[0])
        elif len(city_state) == 2:
            result =city_state[0], shorten_state(city_state[1])
        else:
            result = None, "Anywhere US"
    return result

