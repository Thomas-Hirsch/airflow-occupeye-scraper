# airflow-occupeye-scraper

The Occupeye Airflow Scraper scrapes data from the Occupeye API into an Amazon S3 bucket. This code is run on a daily basis with the `occupeye_scraper_daily` Airflow DAG.

The scraper is run via the `main.py` script in the `python_scripts` folder. It does the following:

- Retrieves the list of Surveys from the API
- Turns it into a table and uploads to S3
- Loops through each survey in the list and:
  - If it's not been scraped before, scrape the entire survey to backfill data
  - Otherwise, scrape the sensor dimensions (metadata) and the previous day's observations
- Refresh the Athena partitions for the `occupeye_db_live` database.

This data ultimately is turned into a database via Amazon Athena, for use in analysis including the Shiny app. In addition to the scraper, this repository also contains code related to the specification of the Athena database.

There is a Jupyter Notebook, `helpfulf_functions.ipynb`, which contains a few useful functions. In particular, you may need to scrape a survey again - either the metadata needs updating, some data went missing, or a new survey has been added and the Estates team needs to report on it the same day.
