import pytest
#from jobs_data_lake.helper import extract_salaries
import helper


def test_extract_salaries_returns_list():
    """
    Test that the scrape_linkedin_jobs function returns a list.
    """
    results = helper.extract_salaries("""salaries and return list from a text example:Salesforce welcomes all.For Colorado-based roles, the base salary hiring range for this 
                               position is $129,800 to $184,200.Compensation offered will be determined by factors such as location, level, job-related knowledge, 
                               skills, and experience.Certain roles may be eligible for incentive compensation, equity, and benefits. More details about our company 
                               benefits can be found at the following link: https://www.salesforcebenefits.com.
      Salary Range: * $129,000 - $168,200. This range is based on national market data and may vary by location.Benefits World Class Benefits: Medical, 
      Prescription, Dental, Vision, Telehealth Health Savings and Flexible Spending Accounts 401(k) and Salary Range $ 109,000 - $ 164,200. This range is based on 
      national market data and may vary by location.Benefits World Class Benefits: Medical, Prescription, Dental, Vision, Telehealth Health Savings and Flexible 
      Spending Accounts 401(k) and avg salary $170,000, and $160.000 And $ 125,000""")
    # print(results)
    assert len(results) == 9


def test_split_location_list():
    """
    Test that the scrape_linkedin_jobs function returns a list.
    """
    locations = ['North Carolina, United States', 'California, US', 'Miami-Fort Lauderdale Area', 'Phoenix, AZ',  'Linthicum, MD', 'United States', 'United States', 'New York, NY',
                 'Manhattan Beach, CA',   'Santa Clara, California','US']
    results = []
    for loc in locations:
        results.append(helper.split_location(loc))
    assert len(results) > 5

# def test_scrape_linkedin_jobs_returns_jobs():
#     """
#     Test that the scrape_linkedin_jobs function returns a list of job dictionaries.
#     """
#     results = scrape_linkedin_jobs()
#     assert all(isinstance(job, dict) for job in results)

# def test_scrape_linkedin_jobs_job_details():
#     """
#     Test that each job dictionary returned by scrape_linkedin_jobs contains
#     the keys "title", "company", "location", "link", and "description".
#     """
#     job_keys = ["title", "company", "location", "link", "description"]
#     results = scrape_linkedin_jobs("software engineer", "san francisco")
#     for job in results:
#         assert all(key in job for key in job_keys)


# def test_scrape_linkedin_jobs_pages():
#     """
#     Test that the scrape_linkedin_jobs function returns at least one job
#     when passed the "pages" argument.
#     """
#     results = scrape_linkedin_jobs("software engineer", "san francisco", pages=2)
#     assert len(results) > 0


# def test_scrape_linkedin_jobs_job_titles():
#     """
#     Test that the titles of all jobs returned by scrape_linkedin_jobs contain the
#     search query passed in the "job_title" argument.
#     """
#     job_title = "software engineer"
#     results = scrape_linkedin_jobs(job_title, "san francisco")
#     assert all(job_title.lower() in job["title"].lower() for job in results)


# def test_scrape_linkedin_jobs_job_locations():
#     """
#     Test that the locations of all jobs returned by scrape_linkedin_jobs contain the
#     search query passed in the "location" argument.
#     """
#     location = "san francisco"
#     results = scrape_linkedin_jobs("software engineer", location)
#     assert all(location.lower() in job["location"].lower() for job in results)
