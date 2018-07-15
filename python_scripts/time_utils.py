import datetime

def num_days_between_scrape_ts_and_now(scrape_datetime):
    now_dt = datetime.datetime.now()
    td = (now_dt - scrape_datetime)
    return td.total_seconds() / (60*60*24)

def scrape_date_in_surveydays(scrape_date_string, survey):
    return (survey["StartDate"] <= scrape_date_string <= survey["EndDate"])