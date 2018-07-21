from s3_utils import read_json_from_s3
import json
import logging
import requests
import urllib

logger = logging.getLogger(__name__)


def get_headers():
    url = base_url + "token"

    headers = {"Grant_type": "password",
               "Username": secrets["Username"],
               "Password": secrets["Password"]}

    r = requests.post(url, data=headers)
    token = json.loads(r.text)

    headers = {"Authorization":"Bearer {}".format(token["access_token"])}
    return headers

def get_surveys_from_api():
    """
    Gets surveys from surveys endpoint
    Returns list of dicts.
    """

    url = base_url + "justice/api/surveys"
    logger.info(f"Getting URL: {url}")
    r = requests.get(url, headers=headers)
    surveys = json.loads(r.text)

    if len(surveys) == 0:
        raise ValueError("No surveys were returned by the API")

    keys = list(surveys[0].keys())
    some_expected_keys = ['SurveyID', 'Active', 'Disabled', 'Name']
    if len( set(some_expected_keys).intersection(set(keys)) ) != 4:
        raise ValueError("Surveys API schema appeas to have changed")

    return surveys

def get_sensors_dimension_from_api(survey):
    """
    Gets sensors from surveydevices endpoint
    Returns list of dicts.
    """
    surveyid = survey["SurveyID"]
    url = base_url + f"Justice/api/SurveyDevices/?surveyid={surveyid}"
    logger.info(f"Getting URL: {url}")
    r = requests.get(url, headers=headers)
    data = json.loads(r.text)

    if len(data) == 0:
        return None

    keys = list(data[0].keys())
    some_expected_keys = ['Building', 'HardwareID', 'SensorID', 'PIRAddress']
    if len( set(some_expected_keys).intersection(set(keys)) ) != 4:
        raise ValueError(f"Surveys API schema appeas to have changed.  Current keys are {keys}")

    return data


def get_survey_facts_from_api(survey, scrape_date_string=None):
    """
    Get sensor facts (observations) from sensoractivity endpoint
    If obs_date passed, gets facts for one date only
    """
    urlparams = {}
    keys = ["StartTime", "StartDate", "EndDate", "EndTime", "SurveyID"]
    for k in keys:
        urlparams[k] = survey[k]

    if scrape_date_string:
        urlparams["StartDate"] = scrape_date_string
        urlparams["EndDate"] = scrape_date_string

    urlparams["Deployment"] = "Justice"
    urlparams["QueryType"] = "SensorActivity"

    url = base_url + "Justice/api/Query?" + urllib.parse.urlencode(urlparams, True)
    logger.info(f"Getting URL: {url}")
    r = requests.get(url, headers=headers)
    data = json.loads(r.text)
    if len(data) == 0:
        return None

    keys = list(data[0].keys())
    some_expected_keys = ['CountOcc', 'CountTotal', 'SurveyDeviceID', 'TriggerDate']
    if len( set(some_expected_keys).intersection(set(keys)) ) != 4:
        raise ValueError(f"Surveys API schema appeas to have changed.  Current keys are {keys}")

    return data

base_url = "https://cloud.occupeye.com/OccupEye/"
secrets = read_json_from_s3("s3://alpha-dag-occupeye/api_secret/api_secrets.json")
headers = get_headers()