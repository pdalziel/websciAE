import json
import time
import tweepy
from pymongo import MongoClient
from tweepy import API, Stream


class SampleStreamListener(tweepy.StreamListener):
    """Not compatible with Python 3.7 due to tweepy implementation of SampleStreamListener
    overrides tweepy.StreamListener to add logic"""
    def __init__(self, logger, mongo_host, collection_id, time_limit,  api=None):
        self.logger = logger
        self.mongo_host = mongo_host
        self.collection_name = collection_id
        self.limit = time_limit
        self.api = api or API()
        self.logger.info("Started")
        self.logger.info(" @ db: " + str(mongo_host) + " on Port: " + str(MongoClient.PORT))
        self.start_time = time.time()

    def on_connect(self):
        """Called once connected to streaming server.
        This will be invoked once a successful response
        is received from the server. Allows the listener
        to perform some work prior to entering the read loop.
        """
        self.logger.info("Connected")

    def on_data(self, data):
        """Called when raw data is received from connection.
        Override this method if you wish to manually handle
        the stream data. Return False to stop stream and close connection.
        """
        try:
            if time.time() >= self.start_time + self.limit:
                self.logger.info("Time limit reached")
                return False  # kill the stream

            client = MongoClient(self.mongo_host)
            db = client.twitterdb
            datajson = json.loads(data)
            #db[self.collection_name].insert(datajson)
            print(self.collection_name + " : " + str(datajson))
        except Exception as e:
            self.logger.info("Exception " + str(e))
            print(e)

    def on_error(self, status_code):
        """Called when a non-200 status code is returned"""
        self.logger.info("Error " + str(status_code))
        print(str(status_code))
        # deal with rate limit if tweepy fails to
        if status_code == 420:
            time.sleep(900)
        return True  # Don't kill the stream

    def on_timeout(self):
        self.logger.info("Timeout")
        return True  # Don't kill the stream


