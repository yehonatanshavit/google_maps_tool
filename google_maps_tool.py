"""
put here all related functions / classes of the project

Update 04/07:
TODO:
    1. add warnings / break cases for the class in the pipeline
    2. write a jupyter notebook
    3. add documentation
    4. write unit-tests (?)
    5. create database - synthetic / combinations of many businesses
"""
import warnings

import googlemaps
import json
import os
import pandas as pd


def read_json_file(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)
    return data


class googlemaps_scraper:
    """
    scrape reviews from google maps using googlemaps library.
    Note:
        1. must have a valid API key from google in order to scrape data,
         read here how to get it https://developers.google.com/maps/documentation/javascript/get-api-key
        2. google limits the amount of reviews that you can scrape from a business to 5
    """

    def __init__(self, api_key: str = None):

        # stage 1: get API key either from input / configuration file
        if api_key is None:
            if not os.path.isfile("key_configs.json"):
                raise Exception("The user did not provide api_key,"
                                " and there is no configuration file, can not proceed")
            keys_dict = read_json_file('key_configs.json')
            if 'google' not in keys_dict.keys():
                raise Exception("The configuration file does not contains google api key")
            api_key = keys_dict['google']
        self.api_key = api_key

        # stage 2: create API
        self.gmaps = googlemaps.Client(key=self.api_key)

        # stage 3: create an empty dictionary for scraped data
        self.names_data = {}
        self.raw_data = []
        self.businesses_data_keys = ['name', 'place_id', 'url', 'rating', 'business_status', 'formatted_address']
        self.business_data = []
        self.reviews_data = []
        self.sample_index = 0

    def __call__(self, places_names: list = [], accumulate: bool = False):
        # create names places names indexes
        for place_name in places_names:
            self.get_raw_data(place_name, accumulate=accumulate)
            self.get_business_data(place_name)

    def get_raw_data(self, place_name: str = None, accumulate: bool = False):
        """
        get raw data about the business from scraping
        """

        # stage 1: get scraped data, and make that it is OK
        scraped_data = self.gmaps.places(place_name)
        if (scraped_data['status'] != "OK") | (len(scraped_data['results']) == 0):
            warnings.warn(f"The business name '{place_name}' can not be extracted from google maps")
            return

        # if there is more than a single results, and the user does not wishes to accumulate the results, get only the
        # first results
        if (not accumulate) & (len(scraped_data['results']) > 1):
            warnings.warn(f"The business name '{place_name} has {len(scraped_data['results'])} results."
                          f" Since 'accumulate==False', using only the first results")
            scraped_data['results'] = [scraped_data['results'][0]]

        # iterate over all results
        for result in scraped_data['results']:

            # make sure that the scraped business name matches the input name
            if result['name'].lower() not in place_name.lower():
                warnings.warn(f"The required business name '{place_name}',"
                              f" does not match the scraped business name '{result['name']}'")

            # stage 2: get business full data using business's ID
            place_data = self.gmaps.place(result['place_id'])

            # make sure place data is valid
            if place_data['status'] != 'OK':
                warnings.warn(f"Something is wrong with extracting data from the place '{place_name}'")
                continue

            # append to raw data
            self.raw_data.append({**{'name_key': place_name}, **place_data['result']})

    def get_business_data(self, place_name: str = None):
        """
        get the business data as mentioned in the required keys
        """

        # iterate over all samples in raw data
        for d in self.raw_data:
            # continue only if the sample is related to current place name
            if d['name_key'] == place_name:
                # increase sample index by 1 (similar to 'primary key' in SQL)
                self.sample_index += 1
                # extract business 'meta' data
                self.business_data.append({**{'business_key': self.sample_index, 'name_key': place_name},
                                           **{k: d[k] for k in self.businesses_data_keys}})
                # extract reviews data
                self.reviews_data.extend([{**{'business_key': self.sample_index, 'name_key': place_name},
                                           **r} for r in d['reviews']])

    def export_data(self):
        pd.DataFrame(self.business_data).to_csv('business_data.csv')
        pd.DataFrame(self.reviews_data).to_csv('reviews_data.csv')


def use_googlemaps_scraper():
    places = ['Subway, New-York', "McDonald's, New-York", "KFC, New-York", "Wendy's, New-York"]
    g_scraper = googlemaps_scraper()
    g_scraper(places, accumulate=True)
    g_scraper.export_data()


if __name__ == "__main__":
    use_googlemaps_scraper()
