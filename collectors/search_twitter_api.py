import pymongo
import collect_twitter_stream
import json
import tweepy
import sys

from pymongo import MongoClient


def connect_mongo(mongo_host=None):
    client = MongoClient('mongodb://localhost/')
    db = client.twitterdb
    return db



def search_api(api, collection_name, tags=None, date_range=None, geocode=None):
    # We can restrict the location of tweets using place:id
    # We can search for multiple phrases using OR
    place = 'place:96683cc9126741d1'
    t = " OR ".join(tags)
    print(t)
    searchQuery = place + t
    db = connect_mongo()
    for page in tweepy.Cursor(api.search, q=searchQuery, lang='en').pages():
        for tweet in page:
            print(tweet._json)

            db[collection_name[0]].insert(tweet._json)
        #db[self.collection_name].insert(datajson)

    # print(my_tweet_list)


# TODO refactor to accept args
# TODO store results in db
# TODO geo search
# TODO store geo search


def get_api():
    auth = collect_twitter_stream.get_auth()
    auth_api = tweepy.API(auth, wait_on_rate_limit=True,
                          wait_on_rate_limit_notify=True)

    if not auth_api:
        print("Problem connecting to API")
        sys.exit(-1)
    return auth_api


if __name__ == '__main__':
    collection = ['search_api']
    tags = ['#brexit', '#Tory']  # this is what we're searching for
    maxTweets = 100  # Some arbitrary large number
    tweetsPerQry = 100  # this is the max the API permits
    dates = ['2018-20-10', '2018-25-11']

    api = get_api()
    try:
        search_api(api, collection, tags, dates)
    except tweepy.TweepError as e:
        # Just exit if any error
        print("some error : " + str(e))
