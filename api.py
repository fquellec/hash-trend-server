from flask import Flask, request, jsonify
from waitress import serve
import pymongo
import credentials
import datetime
import redis
from rq import Queue
from make_report import make_report
from worker import conn

DEBUG_MODE = True

app = Flask(__name__)
q = Queue('main', connection=conn)

@app.route("/")
def handleQuery():
    query = request.args.get('query')

    if query is None:
        return "please add a 'query' parameter"

    client = pymongo.MongoClient(credentials.MONGO_URI)
    db = client.hashTrend
    queries = db.queries
    db_entry = queries.find_one({'query': query})

    if db_entry is not None:
        if db_entry['code'] == 200:
            return db_entry['result'], db_entry['code']
        else:
            return db_entry['status'], db_entry['code']
    else:     
        query_db = {
            "query": query,
            "status": "Fetching Tweets...",
            "code": 202,
            "result": {},
            "date": datetime.datetime.utcnow()
        }

        queries.insert_one(query_db)
        job = q.enqueue(make_report, args=(query,), job_timeout=500)
        return query_db['status'], query_db['code']

# For testing only
@app.route("/db")
def handleDB():
    client = pymongo.MongoClient(credentials.MONGO_URI)
    db = client.hashTrend
    queries = db.queries
    db_entries = queries.find({})
    return str(list(db_entries))


if __name__ == "__main__":
    if DEBUG_MODE:
        app.run(host="127.0.0.1", port=8080, debug=True)
    else:
        serve(app, host="0.0.0.0", port=80)