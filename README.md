# Job Market USA

I tried to capture data from LinkedIn, but unfortunately, LinkedIn is smarter than me and blocked some requests, so I was only able to capture a part of the information. Nonetheless, I believe it should be enough for the final project of DataZoomCamp.

Tech stack:
Scraping: Python (using Selenium and BeautifulSoup libraries)
Data Lake: Prefect, Google Cloud Storage, Google BigQuery
DWH, DM: Google BigQuery, DBT
Reporting: Google Data Studio

## Data Ingestion Schema
![Untitled Diagram drawio](https://user-images.githubusercontent.com/123039991/228688243-7c8eba31-fb2f-4bb1-8d2a-26cc107c14af.png)

## Analytics schema from DBT
<img width="1342" alt="Screenshot 2023-03-29 at 2 28 15 PM" src="https://user-images.githubusercontent.com/123039991/228687724-941464ae-8b02-482c-beeb-cd1a7a90d49f.png">

## Report from Google Data Studio
![2023-03-29 16 10 34](https://user-images.githubusercontent.com/123039991/228688082-557b83dc-acc6-4cff-b6ef-00688027f806.jpg)

Link https://lookerstudio.google.com/s/nTTQAND75yE

### Conclusion
It was an interesting journey. I definitely want to improve the project further by:

- Improving the scraping of LinkedIn
- Investigating other sources
- Adding the ability to manage data from other sources
- Building more sophisticated models and reports
- Creating a bot for Telegram.
