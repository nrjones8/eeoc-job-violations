import csv
import datetime
import hashlib
import logging
import os.path
import requests
import time

from bs4 import BeautifulSoup
from craigslist import CraigslistJobs

"""
https://www.eeoc.gov/laws/practices/inquiries_arrest_conviction.cfm
but where can you file a complaint?
"""
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SEARCH_TERMS = {
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
    'pass drug and background check',
    # '"clean criminal record"', # included above
    'clean background',
    'clean record',
    # '"clean criminal record"', # included above
    # 'no felonies', # included above
    # '"no misdemeanors"',included above
    'pass a background check'
}

# These come from https://geo.craigslist.org/iso/us/ca
# Can parse them out by running (as of 7/6/2020):
# $x('//*[@class="height6 geo-site-list"]//li//a').map(x => x.href.replace('https://', '').replace('.craigslist.org/', ''))
CALIFORNIA_CL_SITES = [
    "bakersfield", "chico", "fresno", "goldcountry", "hanford", "humboldt", "imperial",
    "inlandempire", "losangeles", "mendocino", "merced", "modesto", "monterey", "orangecounty",
    "palmsprings", "redding", "reno", "sacramento", "sandiego", "slo", "santabarbara", "santamaria",
    "sfbay", "siskiyou", "stockton", "susanville", "ventura", "visalia", "yubasutter"
]

IL_CL_SITES = [
    "bn", "chambana", "chicago", "decatur", "lasalle", "mattoon", "peoria", "quadcities", "rockford",
    "carbondale", "springfieldil", "stlouis", "quincy"
]

class FlaggedPost(object):
    def __init__(self, url, job_name, job_body, post_time, flagged_terms, flagged_section_indices):
        self.url = url
        self.job_name = job_name
        self.job_body = job_body
        self.post_time = post_time
        self.flagged_terms = flagged_terms
        self.flagged_section_indices = flagged_section_indices

    def serialize(self):
        serialized = 'Job posted at {}\n'.format(self.post_time)
        serialized += '{}\n'.format(self.url)
        serialized += '{}\n'.format(self.job_name)
        for i in range(len(self.flagged_terms)):
            flagged_index = self.flagged_section_indices[i]
            surrounding_context = self.job_body[flagged_index - 40: flagged_index + 40].encode('ascii', 'ignore')
            serialized += 'Flagged term: "{}"\n'.format(self.flagged_terms[i])
            # TODO err fix this, UnicodeEncodeError errors
            serialized += 'Context of term: "{}"\n\n'.format(surrounding_context)


        return serialized

    def to_dict_for_csv(self):
        return {
            'url': self.url,
            'job_name': self.job_name,
            'job_body': self.job_body,
            'post_time': self.post_time,
            'flagged_terms': ','.join(self.flagged_terms)
            # 'flagged_section_indices': ','.join(self.flagged_section_indices),
        }

def write_cl_post_to_file(path, content):
    with open(path, 'w') as f:
        f.write(str(content))


def read_cl_post_from_file(path):
    with open(path, 'r') as f:
        content = f.read()

    return content


def process_posting(post, dubious_terms, local_base_path, ignore_cache=False, verbose=False):
    """
    :param: `post` is a dict, one of the dicts returned from calling `get_results` on a
    CraigslistJobs object
    :param: `dubious_terms` is a set of terms that are "dubious" in that they often discriminate
    against people with criminal records. Posts that include these terms will be "flagged" for
    review.
    :param: local_path is a path to save the webpage to
    """
    job_name = post['name']
    post_url = post['url']
    post_time = post['datetime']


    # check if file exists already, if it does, don't re-request it, just read it from disk
    # hash the URL, save to a file based on that hash
    hashed_url = hashlib.md5(post_url.encode()).hexdigest()
    cached_path = '{}/{}'.format(local_base_path, hashed_url)
    if os.path.isfile(cached_path) and not ignore_cache:
        logger.info('Reading from cached path {}'.format(cached_path))
        raw_content = read_cl_post_from_file(cached_path)
    else:
        logger.info('Not found locally, requesting from {}'.format(post_url))
        resp = requests.get(post_url)
        raw_content = resp.content
        write_cl_post_to_file(cached_path, raw_content)

    # Whether we got it from the interwebs or locally, parse `raw_content` now
    soup = BeautifulSoup(raw_content, 'html.parser')
    content = soup.find('section', {'id': 'postingbody'})

    if content is None:
        if verbose:
            print('Hm, no content for {}'.format(post_url))
        return None

    flagged_terms = []
    flagged_indices = []

    body_text = content.text.lower()
    for term in dubious_terms:
        starting_index = body_text.find(term)
        if starting_index != -1:
            flagged_terms.append(term)
            flagged_indices.append(starting_index)

    if len(flagged_terms) > 0:
        return FlaggedPost(post_url, job_name, body_text, post_time, flagged_terms, flagged_indices)
    
    print('did not find anything dubious in {}'.format(post_url))
    print(content.text.lower())

    return None

def write_posts_with_metadata(csv_path, post_objs):
    datetime_accessed = datetime.datetime.now()
    # don't write out the same posts more than once
    post_urls_written = set()
    column_names = list(post_objs[0].to_dict_for_csv().keys()) + ['time_accessed']

    with open(csv_path, 'w') as f:
        writer = csv.DictWriter(f, column_names)
        writer.writeheader()
        for post in post_objs:
            post_url = post.url
            if post_url not in post_urls_written:
                to_write = {'time_accessed': str(datetime_accessed)}
                to_write.update(post.to_dict_for_csv())
                writer.writerow(to_write)
                post_urls_written.add(post_url)
            else:
                logger.info('Skipping {} because already wrote it'.format(post_url))

    print('Done writing {} posts to {}'.format(len(post_urls_written), csv_path))

def _build_query_from_list_of_terms(terms):
    quoted_terms = []

    # Quote the multiple word terms. There is probably a way to do this with a list
    # comprehension...
    for t in terms:
        if ' ' in t:
            quoted_terms.append('"{}"'.format(t))
        else:
            quoted_terms.append(t)
    return '|'.join(quoted_terms)


def main():
    # Config stuff
    max_posts = 2000
    sleep_time_between_cities = 10
    ignore_cache = True


    all_flagged_posts = []
    # TODO - make this just concat-ing the TERMS thing from above
    # one_term = 'arrest|conviction|"clean record"|criminal|felony|felonies|misdemeanor|jail|prison|parole'
    one_term = _build_query_from_list_of_terms(SEARCH_TERMS)
    time_started = datetime.datetime.now()

    outfile_name = '{}_{}_{}_{}_{}_potential_eeoc_violations_from_craigslist.csv'.format(
        time_started.year, time_started.month, time_started.day, time_started.hour,
        time_started.minute
    )
    logger.info('Search query is {}'.format(one_term))
    logger.info('Will write output to {}'.format(outfile_name))

    for site in IL_CL_SITES:
        cl_jobs = CraigslistJobs(
            site=site,
            filters={
                'query': one_term
            }
        )

        dubious_posts = []

        num_processed = 0
        for job in cl_jobs.get_results(limit=max_posts, sort_by='newest'):
            potentially_dubious = process_posting(
                job, SEARCH_TERMS, 'cl_posts', ignore_cache=ignore_cache
            )
            if potentially_dubious is not None:
                dubious_posts.append(potentially_dubious)

            num_processed += 1
            if num_processed % 100 == 0:
                print('Done processing {}'.format(num_processed))

        print('{} --- {} of {} posts were flagged as dubious'.format(site, len(dubious_posts), num_processed))
        all_flagged_posts.extend(dubious_posts)
        time.sleep(sleep_time_between_cities)

    write_posts_with_metadata(outfile_name, all_flagged_posts)

if __name__ == '__main__':
    main()
