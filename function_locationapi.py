#!/usr/bin/env python
'''Uses glassdoor advertised job location to search Ordinance Survey API for full location

Keyword arguments:
    scrapedate -- the date of website scrape as it is in the data filename e.g. '14Dec2020'
    path -- path to project data folder; will be searched for glassdoor jobs data file
    verbose -- stream log messages to stdout (default: False)

Returns:
    CSV file of the jobs dataframe with extra columns with the parsed location data from the OS API
'''

# import packages and modules
import argparse
import pandas as pd
import numpy as np
import os
import requests
import logging
import time
from tqdm import tqdm
from fuzzywuzzy import fuzz
from dotenv import load_dotenv

load_dotenv()
api_key = os.environ['OS_API_KEY']


def get_locations(scrapedate, path, verbose=False, api_key=api_key):
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

    # create the full path to the scraped and checked glassdoor job data
    filename = f'gdjobs_df_{scrapedate}_checked.csv'
    filepath = os.path.join(path, filename)

    # read the jobs data (CSV file) into a dataframe
    df = pd.read_csv(filepath, header=0)

    # set up logger
    logger = logging.getLogger(__name__)
    datetime = time.strftime('%d%h%Y_%H%M%S', time.localtime())
    loggerfilename = os.path.join(
        path,
        f'gdjobs_df_{scrapedate}_locationparsed_{datetime}.log'
    )
    file_handler = logging.FileHandler(loggerfilename)
    logger.addHandler(file_handler)
    formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
    file_handler.setFormatter(formatter)

    if verbose:
        logger.setLevel(logging.DEBUG)
        stream_handler = TqdmLoggingHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)
    else:
        logger.setLevel(logging.INFO)

    # create new columns in df for parsed location data, nan by default
    df['api_citytownvilham'] = np.nan  # city/town/village/hamlet etc
    df['api_region'] = np.nan
    df['api_country'] = np.nan
    df['uk'] = False  # if the scraped job location is simply 'United Kingdom'
    df['remote'] = False  # if the scraped job location is 'Remote'

    # iterate through the jobs and parse the scraped location data using The OS Name API if necessary
    for index, row in tqdm(df.iterrows(), total=df.shape[0]):
        location = df.loc[index, 'location']
        logger.debug(f'\n\nJOB {index+1}: {location}...\n\n')

        # initialize a flag variable to indicate when the scraped location has been parsed
        matchfound = False

        # try splitting the scraped location on commas into constituents to get the first part
        try:
            loc_parts = location.split(', ')
            logger.debug(f'Location for job {index+1} split into constituents = {loc_parts}')
        except Exception as e:
            logger.debug(e)
            continue

        if ('Northern Ireland' in loc_parts):
            logger.debug('The job is in "Northern Ireland"; the OS Names API does not work for Northern Ireland')
            df.loc[index, 'api_region'] = 'Northern Ireland'
            df.loc[index, 'api_country'] = 'Northern Ireland'
            if (loc_parts[0] != 'Northern Ireland'):
                df.loc[index, 'api_citytownvilham'] = loc_parts[0]
            matchfound = True

        query = loc_parts[0]

        if ((query == 'London') or (query == 'City of London') or (query == 'Greater London')):
            df.loc[index, 'api_citytownvilham'] = 'London'
            df.loc[index, 'api_region'] = 'London'
            df.loc[index, 'api_country'] = 'England'
            logger.debug(f'Location query is simply {query}; no need to use OS API')
            matchfound = True

        if ((query == 'England') or (query == 'Scotland') or (query == 'Wales')):
            df.loc[index, 'api_country'] = query
            logger.debug('Only the country has been given; no need to use OS API')
            matchfound = True

        if (query == 'United Kingdom'):
            logger.debug(f'Location query is simply {query} (no specific part of the UK); no need to use OS API')
            df.loc[index, 'uk'] = True
            matchfound = True

        if (query == 'Remote'):
            logger.debug(f'Location query is "{query}" (not a location); no need to use OS API')
            df.loc[index, 'remote'] = True
            matchfound = True

        if matchfound:
            pass
        else:
            logger.debug('Need to use the OS Names API')
            url = f'https://api.os.uk/search/names/v1/find?query={query}&fq=LOCAL_TYPE:Town LOCAL_TYPE:City LOCAL_TYPE:Village LOCAL_TYPE:Hamlet LOCAL_TYPE:Suburban_Area LOCAL_TYPE:Other_Settlement&key={api_key}'
            r = requests.get(url)
            logger.debug(f'GET request status code: {r.status_code}')
            json_data = r.json()
            if (r.status_code == 200):  # The HTTP 200 (OK) status response code indicating that the request has succeeded
                logger.debug('Successful GET request')
                if ('results' in json_data):
                    logger.debug('There are "results" in json recieved in response to the GET request for the query, "{query}"')
                    for i in json_data['results']:
                        if matchfound:
                            break
                        else:
                            for x in [i['GAZETTEER_ENTRY']['NAME1'], i['GAZETTEER_ENTRY'].get('NAME2', '')]:
                                if ((fuzz.partial_ratio(query, x) == 100) or (fuzz.ratio(query, x) > 80)):
                                    logger.debug(f'Query, "{query}" has resulted in a match')
                                    matchfound = True
                                    if ('London' in loc_parts[1:]):
                                        df.loc[index, 'api_citytownvilham'] = query
                                        df.loc[index, 'api_region'] = 'London'
                                        df.loc[index, 'api_country'] = 'England'
                                        break
                                    elif((i['GAZETTEER_ENTRY']['LOCAL_TYPE'] == 'Town') or (i['GAZETTEER_ENTRY']['LOCAL_TYPE'] == 'City') or (i['GAZETTEER_ENTRY']['LOCAL_TYPE'] == 'Village') or (i['GAZETTEER_ENTRY']['LOCAL_TYPE'] == 'Hamlet') or (i['GAZETTEER_ENTRY']['LOCAL_TYPE'] == 'Suburban Area') or (i['GAZETTEER_ENTRY']['LOCAL_TYPE'] == 'Other Settlement')):
                                        df.loc[index, 'api_citytownvilham'] = x
                                        df.loc[index, 'api_region'] = i['GAZETTEER_ENTRY'].get('REGION', np.nan)
                                        df.loc[index, 'api_country'] = i['GAZETTEER_ENTRY'].get('COUNTRY', np.nan)
                                        break
                                    else:
                                        logger.debug(i['GAZETTEER_ENTRY']['LOCAL_TYPE'])
                                        df.loc[index, 'api_citytownvilham'] = np.nan
                                        df.loc[index, 'api_region'] = i['GAZETTEER_ENTRY'].get('REGION', np.nan)
                                        df.loc[index, 'api_country'] = i['GAZETTEER_ENTRY'].get('COUNTRY', np.nan)
                                        break
                            if (df.loc[index, 'api_region'] == 'Eastern'):
                                df.loc[index, 'api_region'] = 'East of England'
                                logger.debug(f'api_region was {df.loc[index, "api_region"]}; it has been changed to "East of England"')
            else:
                logger.debug(f'Query ({query}) failed')

        logger.debug(f'\nFinal Results:\napi_citytownvilham = {df.loc[index, "api_citytownvilham"]}\napi_region = {df.loc[index, "api_region"]}\napi_country = {df.loc[index, "api_country"]}\nuk_nonspecific = {df.loc[index, "uk"]}\nremote = {df.loc[index, "remote"]}\n')

    # create a summary table of the parsed locations in the DataFrame so mistakes can be easily spotted
    logger.debug(pd.DataFrame(df.value_counts(subset=['location', 'api_citytownvilham', 'api_region', 'api_country', 'uk', 'remote'], dropna=False)))

    return df


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(  # scrape date
        "scrapedate",
        type=str,
        help="The date of glassdoor.co.uk scrape, e.g. '14Dec2020'"
    )

    parser.add_argument(  # give path to the project data folder
        "path",
        type=str,
        help="Provide path to the project data folder"
    )

    parser.add_argument(  # if you want scraped information to be logged
        "--verbose",
        "-v",
        action='store_true',
        help="Streams log messages to stdout"
    )

    parser.add_argument(  # if you want scraped information to be logged
        "--apikey",
        type=str,
        help="Provide Ordinance Survey API key; defaults to OS_API_KEY environment variable"
    )

    args = parser.parse_args()

    df_main = get_locations(args.scrapedate, args.path, args.verbose)

    # save results as a CSV file
    filename = os.path.join(args.path, f'gdjobs_df_{args.scrapedate}_locationparsed.csv')
    df_main.to_csv(filename, encoding='utf-8')


if __name__ == '__main__':
    main()
