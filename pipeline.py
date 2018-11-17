import configparser
import json
import logging
import os

import math

import get_tokens
import tweepy


from pymongo import MongoClient

import keyword_stream
import location_stream
import sample_stream
import user_stream
from analysis.measure_analytics import count_total_collected_data



# TODO get sample
# TODO unpack sample
# TODO measure basic analytics
# TODO display results
from search_twitter_api import get_api, search_api

CONFIG_PATH = "docs/config.ini"


def get_project_dir():
    # absolute dir this script is in
    return os.path.dirname(__file__)


def load_config():
    project_dir = get_project_dir()
    config_file = os.path.join(project_dir, CONFIG_PATH)
    config = configparser.ConfigParser()
    config.read(config_file)
    return config


def setup_logger(log_name):
    # setup logging
    path = get_project_dir()
    logfile = path + '/logs/' + log_name
    logger = logging.getLogger()
    handler = logging.FileHandler(logfile)
    formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger


def config_mongodb():
    config = load_config()
    mongo_host = config['DEFAULT']['mongo_host']
    return mongo_host


def get_auth():
    """need to supply own twitter auth in /utils/secret.json
    {"secrets":
        {
            "consumer_key": "",
            "consumer_secret": "",
            "access_token": "",
            "access_token_secret": ""
        }

    }
    """
    secrets_dict = get_tokens.load_access_token()
    consumer_key = secrets_dict.get("consumer_key")
    consumer_secret = secrets_dict.get("consumer_secret")
    access_token = secrets_dict.get("access_token")
    access_token_secret = secrets_dict.get("access_token_secret")
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    return auth


def start_sample_stream():
    stream = sample_stream.collect_streaming_sample()
    stream.sample(languages=['en'])


def start_keyword_stream():
    global keyword_stream
    keyword_stream, tracking_tags = keyword_stream.collect_keyword_stream()
    keyword_stream.filter(track=['#brexit'])


def start_user_stream():
    global user_stream
    user_stream, user_ids = user_stream.collect_user_stream()
    user_stream.filter(follow=['813286', '25073877'])


def start_loc_stream():
    global location_stream
    location_stream, location = location_stream.collect_location_stream()
    location_stream.filter(locations=[-4.516, 55.7225, -3.975, 55.978])


def api_search():
    config = load_config()
    collection = config['SEARCH']['collection']
    tags = config['SEARCH']['tags']
    # Some arbitrary large number
    maxTweets = config['SEARCH']['maxTweets']
    tweetsPerQry = 100  # this is the max the API permits
    # can only search 1 week of history
    dates = config['SEARCH']['dates']
    latitude = float(config['SEARCH']['latitude'])    # geographical centre of search
    longitude = float(config['SEARCH']['longitude'])  # geographical centre of search
    max_range = float(config['SEARCH']['max_range']) # search range in kilometres
    geo = "%f,%f,%dkm" % (latitude, longitude, max_range)
    api = get_api()
    try:
        search_api(api, collection, tags, dates)
    except tweepy.TweepError as e:
        # Just exit if any error
        print("some error : " + str(e))

# TODO dump mongo db to csv
# TODO analysis on csv
# TODO implement geo enhance - Locality sensitive hashing

if __name__ == '__main__':
    #api_search()
    count = count_total_collected_data('loc_stream')
    print(count)
    #start_sample_stream()
    #start_keyword_stream()
    #start_user_stream()
    #start_loc_stream()

