import main
import tweepy
import sys

import os

# Replace the API_KEY and API_SECRET with your application's key and secret.
auth = main.get_auth()

api = tweepy.API(auth, wait_on_rate_limit=True,
                 wait_on_rate_limit_notify=True)

target = '#brexit'  # this is what we're searching for
maxTweets = 100 # Some arbitrary large number
tweetsPerQry = 100  # this is the max the API permits
date = ['2018-20-10', '2018-25-11']
if (not api):
    print("Can't Authenticate")
    sys.exit(-1)


def search(target, date, maxnum=1000):
    my_tweet_list = []
    query='#brexit'
    for page in tweepy.Cursor(api.search, q=query).pages():
        print(page)

    #print(my_tweet_list)



# TODO refactor to accept args
# TODO store results in db
# TODO geo search
# TODO store geo search


try:
    search(target, date)

except tweepy.TweepError as e:
    # Just exit if any error
    print("some error : " + str(e))


