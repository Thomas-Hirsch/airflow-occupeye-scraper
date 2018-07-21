import datetime

def scrape_date_in_surveydays(scrape_date_string, survey):
    return (survey["StartDate"] <= scrape_date_string <= survey["EndDate"])

def next_execution_is_in_future(utc_next_execution_date):
    utcnow = datetime.datetime.now(datetime.timezone.utc)
    return utc_next_execution_date > utcnow