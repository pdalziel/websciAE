import json
import time
import tweepy
from pymongo import MongoClient
from tweepy import API, Stream


class SampleStreamListener(tweepy.StreamListener):
    def __init__(self, logger, mongo_host, time_limit, api=None):
        self.api = api or API()
        self.time = time.time()
        self.limit = time_limit
        self.logger = logger
        self.mongo_host = mongo_host
        self.logger.info("Started")
        self.logger.info(" @ db: " + str(mongo_host) + " on Port: " + str(MongoClient.PORT))

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
        while (time.time() - self.time) < self.limit:
            try:
                client = MongoClient(self.mongo_host)
                db = client.twitterdb
                datajson = json.loads(data)
                db.musk.insert(datajson)
                print(datajson)
            except Exception as e:
                self.logger.info("Exception " + str(e))
                print(e)

        self.logger.info("Ended stream")

    def on_error(self, status_code):
        """Called when a non-200 status code is returned"""
        self.logger.info("Error " + str(status_code))
        return True  # Don't kill the stream

    def on_timeout(self):
        self.logger.info("Timeout")
        return True  # Don't kill the stream


