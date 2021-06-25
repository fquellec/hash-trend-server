from searchTweets import searchTweets
import pymongo
import credentials
import pandas as pd

MIN_NB_TWEETS = 10
MAX_NB_TWEETS = 10000

def make_report(query):

	print(f"Start fetching Tweets for keyword {query}")
	df = searchTweets(query)
	df.date = pd.to_datetime(df.date)

	if df.shape[0] < MIN_NB_TWEETS:
		# Update db
		client = pymongo.MongoClient(credentials.MONGO_URI)
		db = client.hashTrend
		queries = db.queries
		myquery = { "query": query }
		newvalues = { "$set": { "status": "Not enough tweets retrieved", "code": 204, } }
		queries.update_one(myquery, newvalues)
		return 204


	# Compute volume of tweets by hours
	volumes = df.resample('h', on='date').count()[['tweet_id']].reset_index().sort_values(by='date')
	volumes = volumes.rename(columns={'date':'name', 'tweet_id':'value'}).to_dict(orient="records")

	# Generate json report
	result = {
		'generalInfos': {
			'nbTweets': int(df.shape[0]),
			'nbLikes':int(df.likes.sum()),
			'nbRetweets': int(df.retweets.sum()),
			'nbUsers': len(df.user_name.unique()),
		},
		'volumes': volumes,
	}

	# Update db
	client = pymongo.MongoClient(credentials.MONGO_URI)
	db = client.hashTrend
	queries = db.queries
	myquery = { "query": query }
	newvalues = { "$set": { "result": result, "code": 200, "status": "Finished!"} }
	queries.update_one(myquery, newvalues)