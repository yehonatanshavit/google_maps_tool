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
        self.raw_data = {}
        self.businesses_data_keys = ['name', 'place_id', 'url', 'rating', 'reviews', 'business_status',
                                     'formatted_address']
        self.business_data = {}
        self.reviews_data = []

    def __call__(self, places_names: list = []):
        """
        This is the full pipeline for scarping reviews from google maps:

            1. the user provides a list of business names

            for each business:
                1. get raw data - get the business data from google maps using googlemaps API
                2. get business data - get each business data (in this stage, includes the reviews)
                3. get reviews that data - separate the reviews data from business data

            Export:
                create 2 dataframes - one for business data (meta-data about each business) and reviews data. In both
                cases, the key of each raw pointing to the original business name that the user provided.

        """
        for place_name in places_names:
            self.get_raw_data(place_name)
            self.get_business_data(place_name)
            self.get_reviews_data(place_name)

    def get_raw_data(self, place_name: str = None):
        """
        get raw data about the business from scraping
        """

        # stage 1: scrape data and validate it
        scraped_data = self.gmaps.places(place_name)
        # Make sure that the results are OK
        if scraped_data['status'] != "OK":
            pass
            return
        # make sure that there is only a single place scraped
        if len(scraped_data['results']) != 1:
            pass
        # make sure that the scraped business name matches the input name
        if scraped_data['results'][0]['name'] not in place_name:
            pass

        # stage 2: get business full data using business's ID
        place_data = self.gmaps.place(scraped_data['results'][0]['place_id'])
        # make sure place data is valid
        if place_data['status'] != 'OK':
            pass
            return

        # append to raw data
        self.raw_data[place_name] = place_data['result']

    def get_business_data(self, place_name: str = None):
        """
        get the business data as mentioned in the required keys
        """
        if place_name in place_name in self.raw_data.keys():
            self.business_data[place_name] = {k: self.raw_data[place_name][k] for k in self.businesses_data_keys}
        else:
            pass

    def get_reviews_data(self, place_name: str = None):

        # make sure that the business in raw data
        if place_name in place_name in self.business_data.keys():
            # extract reviews
            self.reviews_data.extend([{**{'key': place_name}, **r} for r in self.business_data[place_name]['reviews']])
            # delete reviews data from business so it does not appear twice
            del self.business_data[place_name]['reviews']

    def export_data(self):
        return pd.DataFrame(self.business_data).T, pd.DataFrame(self.reviews_data).set_index('key')


def use_googlemaps_scraper():
    places = ['Anita', 'Shuffle Bar, Tel-Aviv', 'shafel', 'gnjowet532']
    g_scraper = googlemaps_scraper()
    g_scraper(places)
    business_df, reviews_df = g_scraper.export_data()


if __name__ == "__main__":
    use_googlemaps_scraper()
