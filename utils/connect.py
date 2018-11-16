from pymongo import MongoClient

import pipeline


class Connect(object):
    @staticmethod
    def get_connection():
        mongo_host = pipeline.config_mongodb()
        return MongoClient(mongo_host)





