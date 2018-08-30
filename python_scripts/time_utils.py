import datetime

def scrape_date_in_surveydays(scrape_date_string, survey):
    return (survey["StartDate"] <= scrape_date_string <= survey["EndDate"])

def next_execution_is_in_future(utc_next_execution_date):
    utcnow = datetime.datetime.now(datetime.timezone.utc)
    return utc_next_execution_date > utcnow

def get_survey_dates(survey):

    def daterange(start_date, end_date):
        for n in range(int ((end_date - start_date).days + 1)):
            yield start_date + datetime.timedelta(n)

    start_date = datetime.datetime.strptime(survey["StartDate"], "%Y-%m-%d").date()
    end_date_1 = datetime.datetime.now().date() - datetime.timedelta(days=1)
    end_date_2 = datetime.datetime.strptime(survey["EndDate"], "%Y-%m-%d").date()

    if end_date_1 < end_date_2:
        end_date = end_date_1
    else:
        end_date = end_date_2

    return daterange(start_date, end_date)