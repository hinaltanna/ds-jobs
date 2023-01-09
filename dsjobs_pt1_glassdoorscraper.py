#!/usr/bin/env python
"""Scrapes details of UK data scientist jobs from www.glassdoor.co.uk posted /
within the last 30 days"""

import argparse
import os
import sys
import logging
import time
from csv import writer

import pandas as pd
from selenium import webdriver
from selenium.common.exceptions import (ElementClickInterceptedException,
                                        ElementNotInteractableException,
                                        NoSuchElementException)
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from tqdm import tqdm
from webdriver_manager.chrome import ChromeDriverManager


def get_jobs(num_jobs, verbose, path):
    '''Scrapes glassdoor.co.uk for UK data scientist job data, /
    returned as a pandas DataFrame or saved as a CSV file'''

    # a function to validate the number of jobs given
    def validatenumjobs(input):
        if isinstance(input, int):
            if ((input <= 0) or (input > 900)):
                raise ValueError(
                    "num_jobs must be between 1 and 900"
                )
            else:
                return input
        else:
            raise TypeError(
                "num_jobs must be an integer between 1 and 900"
            )

    # a function to validate path if given
    def validatepath(input):
        if input is not None:
            if isinstance(input, str):
                if os.path.isdir(input):
                    return input
                else:
                    raise ValueError("path must be valid directory")
            else:
                raise TypeError(
                    "path must be a string"
                )
        else:
            return input

    # validate num_jobs and path
    num_jobs = validatenumjobs(num_jobs)
    path = validatepath(path)

    # set up logger
    class TqdmLoggingHandler(logging.Handler):
        def __init__(self, level=logging.NOTSET):
            super().__init__(level)

        def emit(self, record):
            try:
                msg = self.format(record)
                tqdm.write(msg)
                self.flush()
            except Exception:
                self.handleError(record)

    logger = logging.getLogger(__name__)
    datetime = time.strftime('%d%h%Y_%H%M%S', time.localtime())
    loggerfilename = os.path.join('data', f'glassdoorscraper3_{datetime}.log')
    file_handler = logging.FileHandler(loggerfilename)
    logger.addHandler(file_handler)
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    file_handler.setFormatter(formatter)

    if verbose:
        logger.setLevel(logging.DEBUG)
        stream_handler = TqdmLoggingHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)
    else:
        logger.setLevel(logging.INFO)

    # if data are to be saved as a CSV, then get date-time and create filename
    if path is not None:
        filename = os.path.join('data', f'glassdoor_scrape_{datetime}.csv')
        logger.debug(f"Path provided so data for {num_jobs} jobs will be written to a CSV file ({filename})")
    else:
        # initialise a list of dictionaries with job data, called jobs
        jobs = []
        logger.debug(f"Path NOT provided so data for {num_jobs} jobs will returned as a pandas DataFrame")

    # initialize the webdriver
    logger.debug('Initializing webdriver')
    options = webdriver.ChromeOptions()
    options.add_argument("--window-size=1120,1000")
    driver = webdriver.Chrome(
        options=options,
        service=Service(ChromeDriverManager().install())
    )
    driver.implicitly_wait(1)   # implicitly_wait puts in a retry loop to check for an element on a set interval (every X milliseconds), to see if an element exists
    url = 'https://www.glassdoor.co.uk/Job/uk-data-scientist-jobs-SRCH_IL.0,2_IN2_KO3,17.htm?fromAge=30'
    driver.get(url)

    # start job counter and next page counter
    job_counter = 0
    nextpage_counter = 0

    # initialise the progress bar
    pbar = tqdm(desc='Progress', total=num_jobs)

    # set today's date
    date = time.strftime('%d %B', time.localtime())
    if date[0] == '0':
        date = date[1:]
    logger.debug(f'Todays date:{date}')

    # while the list of jobs in smaller than the target, keep looking for new job ads
    while job_counter < num_jobs:

        # log page
        logger.info(f"--PAGE {nextpage_counter+1}--...")

        # sleep to let the page load
        time.sleep(4)

        # close the cookies pop-up
        try:
            driver.find_element(by=By.ID, value='onetrust-accept-btn-handler').click()  # clicking the "X"
        except NoSuchElementException:
            pass
        except ElementNotInteractableException:
            pass
        else:
            logger.debug('Closed cookies pop-up')
        finally:
            logger.debug('Checked for cookies pop-up')

        # find the links to all jobs listed on the current page
        job_links = driver.find_elements(by=By.CLASS_NAME, value="react-job-listing")

        # check if all jobs on the page have already been scraped (time to quit)
        try:
            vieweddate = driver.find_elements(by=By.XPATH, value='.//div[@data-test="viewedDate"]')
        except NoSuchElementException:
            logger.debug(f"Tried to check how many jobs with a 'viewed date' are on page {nextpage_counter+1}")
        else:
            if (len(vieweddate) == len(job_links)):
                logger.info(f'ALL JOBS ON PAGE {nextpage_counter+1} HAVE ALREADY BEEN SCRAPED')
                break
            else:
                logger.info(f"{len(vieweddate)} JOBS ON PAGE {nextpage_counter+1} HAVE ALREADY BEEN SCRAPED")
        finally:
            logger.debug('Checked how many jobs on this page have already been viewed')

        # iterate through all the job links on the current page and scrape data
        for job_link in job_links:

            # update job counter
            job_counter += 1
            logger.info(f"\n\nJOB {job_counter} OF {num_jobs}...\n")

            # load job; starts on the JOB tab
            job_link.click()
            time.sleep(3)

            # successfully loading a new job may trigger a "sign-up for emails" pop-up; look for it and close by clicking "X"
            try:
                driver.find_element(
                    by=By.XPATH,
                    value='.//*[@id="JAModal"]/div/div[2]/span').click()
            except NoSuchElementException:
                pass
            else:
                logger.debug('Closed "sign-up for emails" pop-up')
            finally:
                logger.debug('Checked for "sign-up for emails" pop-up')

            # save details from job header and job tab
            # COMPANY NAME
            try:
                company_name = driver.find_element(
                    by=By.XPATH,
                    value='.//div[@class="css-xuk5ye e1tk4kwz5"]'
                ).text
            except NoSuchElementException:
                company_name = -1  # set a "not found" value
                logger.error('NoSuchElementException: Tried to save company name')
            else:
                logger.debug('Company name scraped')
                logger.info(f"Company Name: {company_name}")

            # JOB LOCATION
            try:
                location = driver.find_element(
                    by=By.XPATH,
                    value='.//div[@class="css-56kyx5 e1tk4kwz1"]'
                ).text
            except NoSuchElementException:
                location = -1
                logger.error('NoSuchElementException: Tried to save company name')
            else:
                logger.debug('Job location scraped')
                logger.info(f"Location: {location}")

            # JOB TITLE
            try:
                job_title = driver.find_element(
                    by=By.XPATH,
                    value='.//div[@class="css-1j389vi e1tk4kwz2"]'
                ).text
            except NoSuchElementException:
                job_title = -1
                logger.error('NoSuchElementException: Tried to save job title')
            else:
                logger.debug('Job title scraped')
                logger.info(f"Job Title: {job_title}")

            # from job page
            # JOB DESCRIPTION
            try:
                job_description = driver.find_element(
                    by=By.XPATH,
                    value='.//div[@class="jobDescriptionContent desc"]'
                ).text
            except NoSuchElementException:
                logger.error('NoSuchElementException: Tried to save job description but it is not available')
                logger.debug('Trying to reload the job via job link/button')
                job_link.click()
                time.sleep(3)
                try:
                    job_description = driver.find_element(
                        by=By.XPATH,
                        value='.//div[@class="jobDescriptionContent desc"]'
                    ).text
                except NoSuchElementException:
                    logger.error('NoSuchElementException: Tried to save job description a second time but it is still not available')
                    job_description = -1
            else:
                logger.debug('Job description scraped')
                logger.info(f"Job Description: {str(job_description)[:50]}")

            # SALARY ESTIMATE
            try:
                salary_estimate = driver.find_element(
                    by=By.XPATH,
                    value='.//div[@class="css-y2jiyn e2u4hf18"]'
                ).text
            except NoSuchElementException:
                salary_estimate = -1
                logger.error('NoSuchElementException: Tried to save salary estimate')
            else:
                logger.debug('Salary estimate scraped')
                logger.info(f"Salary Estimate: {salary_estimate}")

            # COMPANY RATING (OVERALL)
            try:
                rating = driver.find_element(
                    by=By.XPATH,
                    value='.//span[@data-test="detailRating"]'
                ).text
            except NoSuchElementException:
                rating = -1
                logger.error('NoSuchElementException: Tried to save company rating (overall)')
            else:
                logger.debug('Company rating (overall) scraped')
            finally:
                logger.info(f"Rating: {rating}")

            # COMPANY DETAILS
            try:  # to find elements with company details
                companydetails = driver.find_elements(
                    by=By.XPATH,
                    value=('//div[@class="d-flex justify-content-start css-daag8o e1pvx6aw2"]')
                )
            except NoSuchElementException:
                logger.error("NoSuchElementException: Tried to save company details in Company tab (company details missing)")
                size = -1
                founded = -1
                type_of_ownership = -1
                industry = -1
                sector = -1
                revenue = -1
            else:
                companydetails_labels = []
                companydetails_values = []
                for i in companydetails:
                    c = i.text
                    x, y = c.split("\n", 1)  # the elements containing company details include both the label and the value
                    companydetails_labels.append(x)
                    companydetails_values.append(y)
                # create a dict of company details so they can be searched
                companydetails_dict = dict(zip(companydetails_labels, companydetails_values))
                # search dict and assign company detail variables
                size = companydetails_dict.get('Size', -1)
                founded = companydetails_dict.get('Founded', -1)
                type_of_ownership = companydetails_dict.get('Type', -1)
                industry = companydetails_dict.get('Industry', -1)
                sector = companydetails_dict.get('Sector', -1)
                revenue = companydetails_dict.get('Revenue', -1)
            finally:
                logger.debug("Finished looking for COMPANY DETAILS")
                logger.info(f"Size: {size}")
                logger.info(f"Founded: {founded}")
                logger.info(f"Type of Ownership: {type_of_ownership}")
                logger.info(f"Industry: {industry}")
                logger.info(f"Sector: {sector}")
                logger.info(f"Revenue: {revenue}")

            # SUBRATINGS
            try:  # to find elements containing subratings
                subratings = driver.find_elements(by=By.XPATH, value=('//span[@class="css-1hszvfg erz4gkm1"]'))
            except NoSuchElementException:
                logger.error("NoSuchElementException: Tried to save subratings (subratings missing)")
                rating_culturevalues = -1
                rating_worklifebalance = -1
                rating_seniormgmt = -1
                rating_compbenefits = -1
                rating_careerops = -1
            else:
                subratings_list = []
                for s in range(len(subratings)):
                    subratings_list.append(subratings[s].text)  # alternates between the labels and values
                # create a dict of subratings so they can be searched
                subratings_dict = dict(zip(subratings_list[0::2], subratings_list[1::2]))
                # search dict and assign company detail variables
                rating_culturevalues = subratings_dict.get('Culture & Values', -1)
                rating_worklifebalance = subratings_dict.get('Work/Life Balance', -1)
                rating_seniormgmt = subratings_dict.get('Senior Management', -1)
                rating_compbenefits = subratings_dict.get('Comp & Benefits', -1)
                rating_careerops = subratings_dict.get('Career Opportunities', -1)
            finally:
                logger.debug("Finished looking for SUBRATINGS")
                logger.info(f"Culture & Values: {rating_culturevalues}")
                logger.info(f"Work/Life Balance: {rating_worklifebalance}")
                logger.info(f"Senior Management: {rating_seniormgmt}")
                logger.info(f"Comp & Benefits: {rating_compbenefits}")
                logger.info(f"Career Opportunities: {rating_careerops}")

            if path is not None:  # if a path has been provided
                job_details = [
                    job_title,
                    salary_estimate,
                    job_description,
                    rating,
                    company_name,
                    location,
                    size,
                    founded,
                    type_of_ownership,
                    industry,
                    sector,
                    revenue,
                    rating_culturevalues,
                    rating_worklifebalance,
                    rating_seniormgmt,
                    rating_compbenefits,
                    rating_careerops,
                ]
                # write data to the results csv file
                with open(filename, 'a+', newline='', encoding='utf-8') as write_obj:
                    # create a writer object from csv module
                    csv_writer = writer(write_obj)
                    # add contents of list as last row in the csv file
                    csv_writer.writerow(job_details)
                    # update log
                    logger.debug(f"Job {job_counter} details written to csv")
            else:
                # add job info to jobs dict for pandas DataFrame creation at the end
                jobs.append({
                    "job_title": job_title,
                    "salary_estimate": salary_estimate,
                    "job_description": job_description,
                    "rating": rating,
                    "company_name": company_name,
                    "location": location,
                    "size": size,
                    "founded": founded,
                    "type of ownership": type_of_ownership,
                    "industry": industry,
                    "sector": sector,
                    "revenue": revenue,
                    "rating_culturevalues": rating_culturevalues,
                    "rating_worklifebalance": rating_worklifebalance,
                    "rating_seniormgmt": rating_seniormgmt,
                    "rating_compbenefits": rating_compbenefits,
                    "rating_careerops": rating_careerops,
                })
                # update log
                logger.debug(f"Job {job_counter} details appended to list as a dictionary for pandas DataFrame creation at the end")

            # reset variables to default (null values)
            job_title = -1
            salary_estimate = -1
            job_description = -1
            rating = -1
            company_name = -1
            location = -1
            size = -1
            founded = -1
            type_of_ownership = -1
            industry = -1
            sector = -1
            revenue = -1
            rating_culturevalues = -1
            rating_worklifebalance = -1
            rating_seniormgmt = -1
            rating_compbenefits = -1
            rating_careerops = -1

            # update the progress bar
            pbar.update(1)

            # check whether you need to continue scraping
            if job_counter == num_jobs:
                logger.debug(f"Target reached: Jobs scraped = {job_counter} | Target = {num_jobs}")
                break

        if (len(vieweddate) == len(job_links)):
            break

        if job_counter == num_jobs:
            logger.debug("TARGET NUMBER OF JOBS SCRAPED")
            break

        # click on the "next page" button
        logger.debug("Trying to click the next page")
        try:
            driver.find_element(
                by=By.XPATH,
                value='.//button[@class="nextButton css-1hq9k8 e13qs2071"]'
            ).click()
            time.sleep(3)
        except NoSuchElementException:
            logger.error(f"NoSuchElementException: Link to next page unavailable; scraping terminated before reaching target number of jobs. Needed {num_jobs}, got {len(jobs)}.")
            break
        except ElementClickInterceptedException:
            logger.error(f"ElementClickInterceptedException: Couldn't click next page button; scraping terminated before reaching target number of jobs. Needed {num_jobs}, got {len(jobs)}.")
            break
        else:
            logger.debug("Clicked next page")
            url_str = driver.current_url
            if "pgc=" in url_str:
                nextpage_counter += 1
            else:
                logger.error('"pgc=" not in URL, likely showing duplicate jobs')
                sys.exit(1)

    # update progress bar
    # pbar.close()

    # if path not given, pandas DataFrame will be returned
    if path is None:
        # convert the jobs dictionary object into a DataFrame
        df = pd.DataFrame(jobs)
        logger.info(f"Data for {num_jobs} scraped and returned as a DataFrame")
        return df
    else:
        logger.info(f"Data for {num_jobs} scraped and saved as a CSV ({filename})")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(  # number of jobs
        "num_jobs",
        type=int,
        help="The number of jobs you want to scrape (max 900)"
    )
    parser.add_argument(  # if you want scraped information to be logged
        "--verbose",
        "-v",
        action='store_true',
        help="Streams log messages to stdout"
    )
    parser.add_argument(  # if you want to save data as a csv file, give path to the project folder
        "--path",
        "-p",
        type=str,
        default=None,
        help="Provide path to the project folder to write scraped data to a CSV; otherwise, a pandas DataFrame will be returned "
    )
    args = parser.parse_args()

    get_jobs(num_jobs=args.num_jobs, verbose=args.verbose, path=args.path)


if __name__ == '__main__':
    main()
