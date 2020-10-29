import collections
import csv
import datetime
import hashlib
import logging
import time

import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC



logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

URL_TEMPLATE = 'https://www.ziprecruiter.com/candidate/search?search={search_term}&location={location_term}'

LOCATIONS_TO_SEARCH = [
    'illinois',
]

SEARCH_KEYWORDS = {
    'arrest',
    'conviction',
    'criminal',
    'crime',
    'felony',
    'felonies',
    'misdemeanor',
    'jail',
    'prison',
    'parole',

    # from J/R
    '"pass drug and background check"',
    # '"clean criminal record"', # included above
    '"clean background"',
    '"clean record"',
    # '"clean criminal record"', # included above
    # 'no felonies', # included above
    # '"no misdemeanors"',included above
    '"pass a background check"'
}

# https://www.ziprecruiter.com/terms
# (ii) using any automated system, including without limitation "robots," "spiders," "offline
# readers," etc., to access the Services in a manner that sends more request messages to the
# ZipRecruiter servers than a human can reasonably produce in the same period of time by using a
# conventional on-line web browser (except that ZipRecruiter grants the operators of public search
# engines revocable permission to use spiders to copy materials from ZipRecruiter.com for the sole
# purpose of, and solely to the extent necessary for, creating publicly available searchable indices
# of the materials, but not caches or archives of such materials);
SLEEP_TIME_SECONDS = 10

JOB_POST_FIELD_NAMES = [
    'job_title',
    'job_organization_name',
    'job_snippet_text',
    'source_url',
    'job_search_term',
    'location_search_term',
    'full_url'
]

JOB_POST_FIELD_NAMES_WITH_ID = ['job_id'] + JOB_POST_FIELD_NAMES

class JobPost(collections.namedtuple('JobPost', JOB_POST_FIELD_NAMES)):
    def generate_job_id(self):
        """
        We'll define a job as being "unique" based on its:
        1. Title
        2. Organization name
        3. Location that we searched for

        So if a job like "Customer Serv Agent" from "Southwest Airlines Co." in "new+york" would be
        considered the same, regardless of when we find it.
        """
        job_key = '{}_{}_{}'.format(
            self.job_title, self.job_organization_name, self.location_search_term
        )
        return hashlib.md5(job_key.encode('utf-8')).hexdigest()

    def to_dict_with_id(self):
        as_dict = self._asdict()
        as_dict['job_id'] = self.generate_job_id()

        return as_dict

def write_csv(dataset, csv_path, column_names):
    """
    dataset - a list of JobPost objects
    csv_path - path to save data to
    column_names - the names of the columns for the CSV, in the desired order of columns in the CSV
    """
    with open(csv_path, 'w') as f:
        writer = csv.DictWriter(f, column_names)
        writer.writeheader()
        for row in dataset:
            writer.writerow(row.to_dict_with_id())

def process_block(block, source_url, job_search_term, location_search_term):
    job_title = block.find('span', {'class': 'just_job_title'}).text

    job_org_link = block.find('a', {'class': 't_org_link'})
    job_org_name = job_org_link.text

    snippet = block.find('p', {'class': 'job_snippet'})
    # Strip leading and trailing whitespace
    snippet_text = snippet.text.encode('ascii', 'ignore').lstrip().rstrip()

    full_job_url = block.find('a', {'class': 'job_link'}).attrs['href']

    job_post = JobPost(
        job_title,
        job_org_name,
        snippet_text,
        source_url,
        job_search_term,
        location_search_term,
        full_job_url
    )

    return job_post

def dedupe_jobs(list_of_jobs):
    id_to_job = {}
    for job in list_of_jobs:
        job_id = job.generate_job_id()
        if job_id not in id_to_job:
            id_to_job[job_id] = job

    return id_to_job.values()

class ContentBlockExtractor:
    def __init__(self, driver, url):
        self.driver = driver
        self.url = url

    def _get_num_jobs_shown(self):
        return len(self._get_jobs_on_page())

    def _get_jobs_on_page(self):
        return self.driver.find_elements_by_css_selector('div.job_content')

    def _scroll_to_bottom(self):
        driver.execute_script('window.scrollTo(0,document.body.scrollHeight)')

    def _scroll_to_element(self, element):
        # idk https://stackoverflow.com/a/41744403
        actions = ActionChains(self.driver)
        actions.move_to_element(element).perform()
        logger.info('theoretically scolled to {}'.format(element))

    def _scroll_a_little_past_element(self, element, extra_px=50):
        self._scroll_to_element(element)
        height = element.location['y']
        self.driver.execute_script('window.scrollTo(0, {});'.format(height + extra_px))

    def get_all_blocks(self):
        logger.info('Getting {}'.format(self.url))
        self.driver.get(self.url)
        current_jobs = []
        while True:
            last_seen_jobs = self._get_jobs_on_page()
            logger.info('seeing {} content blocks'.format(len(last_seen_jobs)))

            # scroll to last visible job
            if len(last_seen_jobs) == 0:
                logger.info('No jobs, breakin')
                break

            # the "scroll'n'sleep"
            self._scroll_a_little_past_element(last_seen_jobs[-1])
            # Wait a few seconds for new jobs to appear
            time.sleep(2)

            # Check for more jobs! If we find more, then continue. Otherwise we'll try looking for
            # the "load more jobs" button
            current_jobs = self._get_jobs_on_page()
            if len(current_jobs) > len(last_seen_jobs):
                logger.info('Found more jobs after scrolling! {} vs {}'.format(len(current_jobs), len(last_seen_jobs)))
                last_seen_jobs = current_jobs
                continue

            # Look for and click the "load more jobs" button
            try:
                WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button.load_more_jobs"))).click()
            except Exception as e:
                logger.info('Load more jobs button did not appear. Exception: {}'.format(str(e)))

            time.sleep(3)
            current_jobs = self._get_jobs_on_page()
            logger.info('Now have {} jobs after scrolling'.format(len(current_jobs)))
            if len(current_jobs) == len(last_seen_jobs):
                logger.info('did not find any more, returning {} jobs'.format(len(current_jobs)))
                break
            else:
                logger.info('Updating {} jobs to {} jobs'.format(len(last_seen_jobs), len(current_jobs)))
                last_seen_jobs = current_jobs

        logger.info('Found {} jobs'.format(len(current_jobs)))
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')

        jobs_from_bs = soup.findAll('div', {'class': 'job_content'})
        logger.info('Found {} using selenium, {} using BS'.format(len(current_jobs), len(jobs_from_bs)))

        return jobs_from_bs


def process_search(driver):
    time_started = datetime.datetime.now()
    outfile_name = '{}_{}_{}_{}_{}_potential_eeoc_violations_from_ziprecruiter.csv'.format(
        time_started.year, time_started.month, time_started.day, time_started.hour,
        time_started.minute
    )
    logger.info('Will write output to {}'.format(outfile_name))
    all_jobs = []

    for location in LOCATIONS_TO_SEARCH:
        for search_term in SEARCH_KEYWORDS:
            url = URL_TEMPLATE.format(search_term=search_term, location_term=location)
            # Extract the content blocks using Selenium
            content_blocks = ContentBlockExtractor(driver, url).get_all_blocks()

            # Now that we have them all, parse out their contents
            for block in content_blocks:
                one_job = process_block(block, url, search_term, location)
                all_jobs.append(one_job)

            logger.info('Sleeping {} second(s)'.format(SLEEP_TIME_SECONDS))
            time.sleep(SLEEP_TIME_SECONDS)

    logger.info('Length of all_jobs {}'.format(len(all_jobs)))
    deduped = dedupe_jobs(all_jobs)
    logger.info('Length of deduped {}'.format(len(deduped)))
    write_csv(deduped, outfile_name, JOB_POST_FIELD_NAMES_WITH_ID)

if __name__ == '__main__':
    driver = webdriver.Chrome('/Users/nick/Downloads/chromedriver')
    try:
        process_search(driver)
    finally:
        driver.quit()
