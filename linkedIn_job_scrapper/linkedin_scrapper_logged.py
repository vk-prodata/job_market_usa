# Import necessary packages for web scraping and logging
import collections
import configparser
import csv
import logging
import math
import os
import re
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import pandas as pd
import random
import time
from datetime import datetime
import urllib.parse
import chromedriver_autoinstaller

PAGE_SIZE = 25
MAX_RETRIES = 3


def login_to_linkedin(username, password):

    # Navigate to LinkedIn login page
    driver.get('https://www.linkedin.com/')
    driver.implicitly_wait(7)
    _username = driver.find_element(By.ID, 'session_key')
    _password = driver.find_element(By.ID, 'session_password')
    _login = driver.find_element(
        By.XPATH, '//*[@id="main-content"]/section[1]/div/div/form/div[2]/button[1]')
    _username.send_keys(username)
    _password.send_keys(password)
    _login.click()
    time.sleep(7)


def prepare_string_for_url(string):
    # Encode the string using URL encoding
    encoded_string = urllib.parse.quote(string)

    # Replace spaces with %20
    encoded_string = encoded_string.replace(' ', '%20')

    # Return the encoded string
    return encoded_string


def scrapping_workflow(loc: str, job_title: str, subfolder: str):
    url_title = prepare_string_for_url(job_title)
    url_loc = prepare_string_for_url(loc)
    query = f"https://www.linkedin.com/jobs/search/?f_T=13447%2C340%2C2732%2C30209%2C30006&f_TPR=r604800&keywords={url_title}&location={url_loc}&refresh=true&sortBy=RR&start="

    retry_count = 0
    success = False
    # open Job page. Sometimes it's failed, added try with 3 retries
    while not success and retry_count < MAX_RETRIES:
        try:
            driver.get(query)
            success = True
        except Exception as e:
            # handle the exception, e.g., print an error message
            logging.info(f"driver get an exception occurred. Retrying...{e=}")
            # increase the retry count
            time.sleep(5)
            retry_count += 1

    if not success:
        logging.info(
            f"driver get failed after {MAX_RETRIES=}")

    logging.info(
        f"Successfully open {query=}")
    time.sleep(random.choice(list(range(3, 10))))
    # We find how many jobs are offered. Nice to have (not mandatory) this info for comparison count of all scrapped positions
    # if we cannot capture jobs count set 1000 - max count that we can retrieve from linkedin UI
    try:
        jobs_num_str = driver.find_element(
            By.CLASS_NAME, "jobs-search-results-list__subtitle").get_attribute("innerText")

        # Search for one or more digits and commas
        match = re.search(r'[\d,]+', jobs_num_str)
        if match:
            jobs_num = int(match.group().replace(',', ''))
            logging.info(
                f"Count of jobs {jobs_num} in {loc=} for {job_title=}")
        else:
            logging.info(
                f"No number found in text {jobs_num_str} in {loc=} for {job_title=}")
            jobs_num = 1000
            print("No number found in text")
        write_number_to_file(job_title, loc, jobs_num, 'jobs_count.csv')
    except Exception as e:
        # Log a message indicating that the button was not found and we're retrying
        logging.info(f"jobs_num cannot retrieve {e=}")
        jobs_num = 1000
    # MAIN SECTION FOR PARSING JOBS
    # We create a while loop to browse all jobs.
    i = 0
    while i <= int(jobs_num/PAGE_SIZE):
        if is_last_page(driver) == True:
            break
        try:
            scrolling_left_section(query+str(i*PAGE_SIZE))
            time.sleep(11)  # we need aboit 11 sec for loading all info
            jobs = get_job_lists_soup(loc)
            i += 1
            driver.get(query+str(i*PAGE_SIZE))
            save_job_data(job_title, loc, 1, jobs, subfolder)
            time.sleep(random.random()*5)
        except:
            # If there is no button, there will be an error, so we keep scrolling down.
            time.sleep(0.8)
            pass


def is_last_page(driver):
    try:
        # t-24 t-black t-normal text-align-center
        no_matching_jobs = driver.find_element_by_tag_name("h1").text
        if no_matching_jobs == "No matching jobs found.":
            return True
    except:
        return False


def scrolling_left_section(full_query):
    retry_count = 0
    success = False
    while not success and retry_count < MAX_RETRIES:
        try:
            if is_last_page(driver) == True:
                retry_count += 1
            jobs_block = driver.find_element(
                By.CLASS_NAME, "jobs-search-results-list")
            jobs_list = jobs_block.find_elements(
                By.CLASS_NAME, "jobs-search-results__list-item")

            if len(jobs_list) == 0:
                retry_count += 1
                driver.get(full_query)
            for j in range(len(jobs_list)):
                driver.execute_script(
                    "arguments[0].scrollIntoView();", jobs_list[j])
                if j in (3, 9, 14, 19, 22):
                    time.sleep(random.random()*10)
            success = True
        except Exception as e:
            # handle the exception, e.g., print an error message
            logging.info(
                f"scrolling get an exception occurred. Retrying...{e=}")
            # increase the retry count
            driver.get(full_query)
            time.sleep(5)
            retry_count += 1

    if not success:
        logging.info(
            f"scrolling get failed after {MAX_RETRIES=}")


def get_job_lists_soup(search_area: str):
    # Scrape the job postings
    jobs = []
    soup = BeautifulSoup(driver.page_source, "html.parser")
    job_listings = soup.find_all(
        "div",
        class_="job-card-container",
    )
    try:
        for i, job in enumerate(job_listings):
            job_title, job_company, location, salary, date_posting = "", "", "", "", ""
            try:
                # Extract job details
                job_title = job.find(
                    "a", class_="disabled ember-view job-card-container__link job-card-list__title").text.strip()
                # "app-aware-link"
                job_company = job.find("span", class_="job-card-container__primary-description"
                                       ).text.strip()
                location = job.find(
                    "div", class_="artdeco-entity-lockup__caption ember-view").text.strip()
                try:
                    salary = job.find(
                        "div", class_="mt1 t-sans t-12 t-black--light t-normal t-roman artdeco-entity-lockup__metadata ember-view").text.strip()
                except Exception as e:
                    print(e)
                try:
                    date_posting = job.find("time")["datetime"]
                except Exception as e:
                    print(e)

                apply_link = job.find(
                    "a", class_="disabled ember-view job-card-container__link job-card-list__title")["href"]
                # Add job details to the jobs list
                jobs.append(
                    {
                        "title": job_title,
                        "company": job_company,
                        "location": location,
                        "date_posting": date_posting,
                        "link": apply_link,
                        "salary": salary,
                        "search_area": search_area,
                    }
                )
            except Exception as e:
                logging.error(
                    f"An error occurred while scraping {i=}: {str(e)}")
                continue
    # Catching any exception that occurs in the scrapping process
    except Exception as e:
        # Log an error message with the exception details
        logging.error(f"An error occurred while scraping jobs: {str(e)}")

        # Return the jobs list that has been collected so far
        # This ensures that even if the scraping process is interrupted due to an error, we still have some data
        return jobs

    # Return the jobs list
    return jobs


def write_number_to_file(job_title: str, loc: str, job_count: int, filename: str):
    # Try to open the file in "append" mode
    try:
        with open(filename, 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            # Write two numbers to CSV row
            writer.writerow([job_title, loc, job_count, datetime.now()])

    # If the file doesn't exist, create it and write the numbers to it
    except FileNotFoundError:
        with open(filename, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Title', 'Location', 'Job Count',
                            'Datetime'])  # Write header row
            # Write two numbers to CSV row
            writer.writerow([job_title, loc, job_count,
                            datetime.now().strftime("%H_%M_%S")])


def save_job_data(job_title: str, loc: str, page_number: int, data: dict, subfolder: str) -> None:
    """
    Save job data to a CSV file.
    Args:data: A dictionary containing job data.
    Returns: None
    """
    # Create a pandas DataFrame from the job data dictionary
    df = pd.DataFrame(data)
    title = job_title.replace(' ', '_')
    ts = datetime.now().strftime("%d-%m-%Y")
    output_file = f"data/{subfolder}/{title}_{loc}_jobs_{page_number}_{ts}.csv"
    if os.path.exists(output_file):
        # If the file already exists, append the DataFrame to it
        df.to_csv(output_file, mode='a', header=False, index=False)
    else:
        # If the file does not exist, save the DataFrame to a new file
        df.to_csv(output_file, index=False)

    # Log a message indicating how many jobs were successfully scraped and saved to the CSV file
    logging.info(
        f"Successfully scraped {len(data)} jobs and saved to jobs.csv")


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read('config.ini')
    username = config['CREDENTIALS']['Username']
    password = config['CREDENTIALS']['Password']
    job_title = "data engineer"
    locations = ["San Francisco, California", "San Jose, California", "Seattle, Washington", "St. Louis, Missouri", "Tampa, Florida", "Washington, DC"]
    locations1 = ["Atlanta, Georgia", "Austin, Texas", "Boston, Massachusetts", "Charlotte, North Carolina", "Chicago, Illinois",
                  "Columbus, Ohio", "Dallas, Texas", "Denver, Colorado", "Houston, Texas", "Indianapolis, Indiana", "Kansas City, Missouri",
                  "Los Angeles, California", "Miami, Florida", "Minneapolis, Minnesota", "Nashville, Tennessee",
                  "New York City, New York", "Orlando, Florida", "Philadelphia, Pennsylvania", "Phoenix, Arizona", "Pittsburgh, Pennsylvania",
                  "Portland, Oregon", "Raleigh, North Carolina", "Salt Lake City, Utah", "San Antonio, Texas", "San Diego, California",
                  "San Francisco, California", "San Jose, California", "Seattle, Washington", "St. Louis, Missouri", "Tampa, Florida", "Washington, DC"]

    # Configure logging settings
    logging.basicConfig(filename="scraping.log", level=logging.INFO)

    # Set up Chrome options to maximize the window
    # options = webdriver.ChromeOptions()
    # options.add_argument("--start-maximized")
    # Initialize the web driver with the Chrome options
    #driver = webdriver.Chrome(options=options)
    chromedriver_autoinstaller.install() 
 
    # Create Chromeoptions instance 
    options = webdriver.ChromeOptions() 
    options.add_argument("--start-maximized")
    
    # Adding argument to disable the AutomationControlled flag 
    options.add_argument("--disable-blink-features=AutomationControlled") 
    
    # Exclude the collection of enable-automation switches 
    options.add_experimental_option("excludeSwitches", ["enable-automation"]) 
    
    # Turn-off userAutomationExtension 
    options.add_experimental_option("useAutomationExtension", False) 
    
    # Initializing a list with two Useragents 
    useragentarray = [ 
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36", 
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36", 
    ] 

    # Setting the driver path and requesting a page 
    driver = webdriver.Chrome(options=options) 
    
    # Changing the property of the navigator value for webdriver to undefined 
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})") 
    #driver.execute_cdp_cmd("Network.setUserAgentOverride", {"userAgent": useragentarray[0]}) 
    subfolder = "02-06-2023"

    login_to_linkedin(username, password)
    for location in locations:
        scrapping_workflow(location, job_title, subfolder)

    driver.quit()
