import tweepy
import sys

import pipeline
import pipeline
from utils.connect import Connect


def connect_mongo():
    conn = Connect.get_connection()
    db = conn.twitterdb
    return db


def search_api(api, collection_name, tags=None, date_range=None,):
    query = " OR ".join(tags)
    db = connect_mongo()
    for page in tweepy.Cursor(api.search, q=query, lang='en').pages():
        for tweet in page:
            print(tweet._json)
            db[collection_name[0]].insert(tweet._json)


def get_api():
    auth = pipeline.get_auth()
    auth_api = tweepy.API(auth, wait_on_rate_limit=True,
                          wait_on_rate_limit_notify=True)

    if not auth_api:
        print("Problem connecting to API")
        sys.exit(-1)
    return auth_api


#if __name__ == '__main__':
    # '55.8642,4.2518'  # glasgow lat and long



# q – the search query string
#         lang – Restricts tweets to the given language, given by an ISO 639-1 code.
#         locale – Specify the language of the query you are sending. This is intended for language-specific clients
#           and the default should work in the majority of cases.
#         rpp – The number of tweets to return per page, up to a max of 100.
#         page – The page number (starting at 1) to return, up to a max of roughly 1500 results (based on rpp * page.
#         since_id – Returns only statuses with an ID greater than (that is, more recent than) the specified ID.
#         geocode – Returns tweets by users located within a given radius of the given latitude/longitude.
#           The location is preferentially taking from the Geotagging API, but will fall back to their Twitter profile.
#           The parameter value is specified by “latitide,longitude,radius”, where radius units must be specified
#           as either “mi” (miles) or “km” (kilometers).
#           Note that you cannot use the near operator via the API to geocode arbitrary locations;
#           however you can use this geocode parameter to search near geocodes directly.
#         show_user – When true, prepends “<user>:” to the beginning of the tweet.
#           This is useful for readers that do not display Atom’s author field. The default is false.
#
#
#
# Resource Information
# Response formats	JSON
# Requires authentication?	Yes
# Rate limited?	Yes
# Requests / 15-min window (user auth)	180
# Requests / 15-min window (app auth)	450
# Parameters
# Name	Required	Description	Default Value	Example
# q	required	A UTF-8, URL-encoded search query of 500 characters maximum, including operators. Queries may additionally
# be limited by complexity.	 	@noradio
# geocode	optional	Returns tweets by users located within a given radius of the given latitude/longitude.
# The location is preferentially taking from the Geotagging API, but will fall back to their Twitter profile.
# The parameter value is specified by ” latitude,longitude,radius “,
# where radius units must be specified as either ” mi ” (miles) or ” km ”
# (kilometers). Note that you cannot use the near operator via the API to geocode arbitrary locations;
# however you can use this geocode parameter to search near geocodes directly.
# A maximum of 1,000 distinct “sub-regions” will be considered when using the radius modifier.
#  	37.781157 -122.398720 1mi
# lang	optional	Restricts tweets to the given language, given by an ISO 639-1 code.
# Language detection is best-effort.	 	eu
# locale	optional	Specify the language of the query you are sending (only ja is currently effective).
# This is intended for language-specific consumers and the default should work in the majority of cases.	 	ja
# result_type	optional
# Optional. Specifies what type of search results you would prefer to receive.
# The current default is “mixed.” Valid values include:
#
# * mixed : Include both popular and real time results in the response.
#
# * recent : return only the most recent results in the response
#
# * popular : return only the most popular results in the response.
#
#  	mixed recent popular
# count	optional	The number of tweets to return per page, up to a maximum of 100. Defaults to 15.
# This was formerly the “rpp” parameter in the old Search API.	 	100
# until	optional	Returns tweets created before the given date. Date should be formatted as YYYY-MM-DD.
# Keep in mind that the search index has a 7-day limit.
# In other words, no tweets will be found for a date older than one week.	 	2015-07-19
# since_id	optional	Returns results with an ID greater than (that is, more recent than) the specified ID.
# There are limits to the number of Tweets which can be accessed through the API.
# If the limit of Tweets has occured since the since_id, the since_id will be forced to the oldest ID available.	 	12345
# max_id	optional	Returns results with an ID less than (that is, older than) or equal to the specified ID.	 	54321
# include_entities	optional	The entities node will not be included when set to false.	 	false
