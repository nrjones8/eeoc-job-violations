import collections
import csv
import datetime
import hashlib
import logging
import time

import requests
from bs4 import BeautifulSoup
from selenium import webdriver

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

URL_TEMPLATE = 'https://www.ziprecruiter.com/candidate/search?search={search_term}&location={location_term}&page={page_num}'


CA_CITIES = [
    'california',
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

    # from Jen / Redlands
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

    # The encoding here is just me being lazy :'(
    job_post = JobPost(
        job_title,
        job_org_name,
        snippet_text,
        source_url,
        job_search_term,
        location_search_term,
        full_job_url
        # job_title.encode('ascii', 'ignore'),
        # job_org_name.encode('ascii', 'ignore'),
        # snippet_text.encode('ascii', 'ignore'),
        # source_url.encode('ascii', 'ignore'),
        # job_search_term.encode('ascii', 'ignore'),
        # location_search_term.encode('ascii', 'ignore'),
        # full_job_url.encode('ascii', 'ignore')
    )

    return job_post

def dedupe_jobs(list_of_jobs):
    id_to_job = {}
    for job in list_of_jobs:
        job_id = job.generate_job_id()
        if job_id not in id_to_job:
            id_to_job[job_id] = job

    return id_to_job.values()

def process_search(driver):
    time_started = datetime.datetime.now()
    outfile_name = '{}_{}_{}_{}_{}_potential_eeoc_violations_from_ziprecruiter.csv'.format(
        time_started.year, time_started.month, time_started.day, time_started.hour,
        time_started.minute
    )
    logger.info('Will write output to {}'.format(outfile_name))
    all_jobs = []

    # search_term = 'felony -driver'
    for location in CA_CITIES:
        for search_term in SEARCH_KEYWORDS:
            page_num = 0
            while True:
                url = URL_TEMPLATE.format(search_term=search_term, location_term=location, page_num=page_num)
                logger.info('Getting {}'.format(url))
                driver.get(url)
                soup = BeautifulSoup(driver.page_source, 'html.parser')                    

                content_blocks = soup.findAll('div', {'class': 'job_content'})
                logger.info('Found {} jobs'.format(len(content_blocks)))
                if len(content_blocks) < 1:
                    logger.info('No more jobs found, moving on')
                    break

                for block in content_blocks:
                    one_job = process_block(block, url, search_term, location)
                    all_jobs.append(one_job)

                logger.info('Sleeping {} second(s)'.format(SLEEP_TIME_SECONDS))
                time.sleep(SLEEP_TIME_SECONDS)
                page_num += 1

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
