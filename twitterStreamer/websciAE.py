import json
import time
import tweepy
from pymongo import MongoClient
from tweepy import API, Stream


class StreamListener(tweepy.StreamListener):
    def __init__(self, logger, mongo_host, time_limit, db, collection, api=None):
        self.api = api or API()
        self.time = time.time()
        self.limit = time_limit
        self.logger = logger
        self.mongo_host = mongo_host
        self.db = db
        self.collection = collection
        self.logger.info("Started")
        self.logger.info("db = " + str(MongoClient.address) + " Port: " + str(MongoClient.PORT))

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
        if (time.time() - self.time) < self.limit:
            print(time.time() - self.time)
            try:
                datajson = json.loads(data)
                self.db.collection.insert(datajson)
                print(datajson)
            except Exception as e:
                self.logger.info("error " + str(e))
        else:
            Stream.disconnect()

    def on_error(self, status_code):
        """Called when a non-200 status code is returned"""
        self.logger.info("Error " + str(status_code))
        return True  # Don't kill the stream

    def on_timeout(self):
        self.logger.info("Timeout")
        return True  # Don't kill the stream


