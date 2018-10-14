import json
import time
import logging
import tweepy
from pymongo import MongoClient
from tweepy import API, Stream

from twitterStreamer import secret

CONSUMER_KEY = secret.CONSUMER_KEY
CONSUMER_SECRET = secret.CONSUMER_SECRET
ACCESS_TOKEN = secret.ACCESS_TOKEN
ACCESS_TOKEN_SECRET = secret.ACCESS_TOKEN_SECRET

MONGO_HOST = 'mongodb://localhost/tweetdb'


class StreamListener(tweepy.StreamListener):
    def __init__(self, time_limit, api=None):
        self.api = api or API()
        self.time = time.time()
        self.limit = time_limit
        logger.info("Started")
        logger.info("db = " + str(MONGO_HOST) + " Port: " + str(MongoClient.PORT))

    def on_connect(self):
        """Called once connected to streaming server.
        This will be invoked once a successful response
        is received from the server. Allows the listener
        to perform some work prior to entering the read loop.
        """
        logger.info("Connected")

    def on_data(self, data):
        """Called when raw data is received from connection.
        Override this method if you wish to manually handle
        the stream data. Return False to stop stream and close connection.
        """
        if (time.time() - self.time) < self.limit:
            print(time.time() - self.time)
            try:
                client = MongoClient(MONGO_HOST)
                db = client.twitterdb
                datajson = json.loads(data)
                db.twitter_sample.insert(datajson)
                print(datajson)
            except Exception as e:
                logger.info("error " + str(e))
        else:
            stream.disconnect()

    def on_error(self, status_code):
        """Called when a non-200 status code is returned"""
        logger.info("Error " + str(status_code))
        return True  # Don't kill the stream

    def on_timeout(self):
        logger.info("Timeout")
        return True  # Don't kill the stream

        "Stream restarted"


if __name__ == '__main__':
    # setup logging
    logger = logging.getLogger()
    handler = logging.FileHandler('/home/paul/PycharmProjects/websciAE/docs/streamlog.log')
    formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    # This handles Twitter authentication and the connection to Twitter Streaming API
    time_limit = 3600

    listener = StreamListener(time_limit, api=tweepy.API(wait_on_rate_limit=True))
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
    stream = Stream(auth, listener)

    # This line filter Twitter Streams to capture data by the keyword:
    stream.sample()
    logger.info("Finished")
