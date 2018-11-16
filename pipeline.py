import configparser
import json
import logging
import os
import get_tokens
import tweepy


from pymongo import MongoClient

import keyword_stream
import location_stream
import sample_stream
import user_stream
from analysis.measure_analytics import count_total_collected_data


#MONGO_HOST = 'mongodb://localhost/twitterdb'

# TODO get sample
# TODO unpack sample
# TODO measure basic analytics
# TODO display results
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


if __name__ == '__main__':
    #coll, client = get_client()
    count = count_total_collected_data('tweets')
    print(count)
    #stream = sample_stream.collect_streaming_sample()
    #stream.sample(languages=['en'])
    #keyword_stream, tracking_tags = keyword_stream.collect_keyword_stream()
    #keyword_stream.filter(track=['#brexit'])
    #user_stream, user_ids = user_stream.collect_user_stream()
    #user_stream.filter(follow=['813286', '25073877'])
    #location_stream, location = location_stream.collect_location_stream()
    #location_stream.filter(locations=[-4.516, 55.7225, -3.975, 55.978])

