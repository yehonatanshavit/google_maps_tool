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
from datetime import datetime, timedelta
import matplotlib.pyplot as plt


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
    # according to google's documentations, the key 'time' in reviews is the amount of seconds since 1/1/1970 midnight.
    # Take a look here: https://developers.google.com/maps/documentation/places/web-service/details#PlaceReview
    start_date = datetime(1970, 1, 1, 0, 0, 0)

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
        self.query_dataset = []
        self.business_dataset = []
        self.reviews_dataset = []

    def create_dataset(self, places_names: list, dataset_name: str = "dataset", export: bool = True):
        self.create_query_table(places_names)
        self.create_business_table()
        self.create_reviews_table()
        if export:
            self.export_tables(dataset_name)

    def create_query_table(self, places_names):

        # create query dataset
        for index, place_name in enumerate(places_names):
            # scrape data
            scraped_data = getattr(self.gmaps, 'places')(place_name)

            # update query dataset
            self.query_dataset.append({'index': index, 'query': place_name, 'status': scraped_data['status'],
                                       'amount_of_results': len(scraped_data['results']), 'date': datetime.now(),
                                       'results': scraped_data['results']})

    def create_business_table(self):
        business_index = 0
        for query_data in self.query_dataset:
            for business_data in query_data['results']:
                business_index += 1
                business_data = getattr(self.gmaps, 'place')(business_data['place_id'])
                self.business_dataset.append({'index': business_index,
                                              'query_id': query_data['index'],
                                              'name': business_data['result']['name'],
                                              'place_id': business_data['result']['place_id'],
                                              'query_status': business_data['status'],
                                              'name_match': business_data['result']['name'].lower() in query_data[
                                                  'query'].lower(),
                                              'url': business_data['result']['url'],
                                              'rating': business_data['result']['rating'],
                                              'business_status': business_data['result']['business_status'],
                                              'address': business_data['result']['formatted_address'],
                                              'n_reviews': len(business_data['result']['reviews']),
                                              'reviews': business_data['result']['reviews']})
            del query_data['results']

    def create_reviews_table(self):
        """
        IMPORTANT - according to this documentation,
        https://developers.google.com/maps/documentation/places/web-service/details#PlaceReview
        the key 'time' in review represents "The time that the review was submitted, measured in the number of seconds
         since since midnight, January 1, 1970 UTC."
        """
        review_index = 0
        for business_data in self.business_dataset:
            for review in business_data['reviews']:
                review_index += 1
                self.reviews_dataset.append({'index': review_index,
                                             'business_id': business_data['index'],
                                             'author_name': review['author_name'],
                                             'author_url': review['author_url'],
                                             'language': review['language'],
                                             'translated': review['translated'],
                                             'rating': review['rating'],
                                             'time': review['time'],  # amount of seconds since 01/01/1970 at midnight
                                             'date': add_seconds(self.start_date, review['time']),
                                             'text': review['text']})
            del business_data['reviews']

    def export_tables(self, dateset_name):
        if not os.path.isdir(dateset_name):
            os.mkdir(dateset_name)
        for table_name in ['query', 'business', 'reviews']:
            pd.DataFrame(getattr(self, f'{table_name}_dataset')).to_csv(f'{dateset_name}/{table_name}.csv')


def add_seconds(start_date, seconds_to_add):
    """
    given start date and amount of seconds, calculate the new date
    """
    return start_date + timedelta(seconds=seconds_to_add)


def run_googlemaps_scraper():
    """

    This function runs the googlemaps_scraper class that creates a dataset with 3 tables.

    User inputs:
        1. businesses to query
        2. dataset name

    Outputs - 3 tables in csv format
        1. query - information about the query processing
        2. business - information about each business that was extracted from each query
        3. reviews - information about each business's review

    """
    dataset_name = "new_york_fast_food"
    places = ['Subway, New-York', "McDonald's, New-York", "KFC, New-York", "Wendy's, New-York", "Wenedies, Nu,York",
              "BulBulAkaBulBul"]
    g_scrape = googlemaps_scraper()
    g_scrape.create_dataset(places, dataset_name, False)

    #
    reviews = pd.DataFrame(g_scrape.reviews_dataset)
    agg_rating(reviews_df=reviews, frequency='Y')#, date_range=('01-01-2015',datetime.today()), min_review=2)


def agg_rating(reviews_df: pd.DataFrame,
               frequency: str = "M",
               date_range: tuple = (googlemaps_scraper.start_date, datetime.today() + timedelta(days=1)),
               min_review: int = 0):

    # filter dates
    reviews_df = reviews_df[(reviews_df['date']>date_range[0]) & (reviews_df['date']<date_range[1])]
    if reviews_df.shape[0] == 0:
        warnings.warn(f"There are no dates between {date_range[0]}-{date_range[1]}. Chose another range")

    # assert frequency
    if frequency.lower() not in ['y', 'm', 'd']:
        raise ValueError("The frequency should be by year (Y) / month (M) / day (D)")

    # get for each month - mean review, STD review, amount of reviews
    ratings_agg_df = reviews_df.resample(rule=frequency, on='date').agg({'rating': ['mean', 'std', 'count']})

    # get only values above minimal amount of reviews
    ratings_agg_df = ratings_agg_df[ratings_agg_df['rating']['count'] >= min_review]

    print(ratings_agg_df)

    # TODO - fix bug: X-axis shows one year ahead (of it's 2023, it shows 2024)
    
    # plot
    plt.plot(ratings_agg_df.index.to_list(), ratings_agg_df['rating']['mean'])
    plt.scatter(ratings_agg_df.index.to_list(), ratings_agg_df['rating']['mean'])
    plt.fill_between(ratings_agg_df.index.to_list(),
                     ratings_agg_df['rating']['mean'] + ratings_agg_df['rating']['std'],
                     ratings_agg_df['rating']['mean'] - ratings_agg_df['rating']['std'], alpha=0.4)
    plt.show()



if __name__ == "__main__":
    run_googlemaps_scraper()

    """
    TODO
        * create data processing
        * create graphics for a single business:
            1. time-trend - all reviews, reviews without-text, reviews with text
            2. percentage of reviews with / without text
            3. percentage of reviews amount [0-1, 1-2, 2-3, 3-4, 4-5]
            4. text analysis
            5. reviewers similar to user analysis
            6. bots analysis
            
        * create graphics for comparing businesses
    """
