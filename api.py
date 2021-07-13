from flask import Flask, request, jsonify
from waitress import serve
import pymongo
import credentials
import datetime
import redis
from rq import Queue
from make_report import make_report
from worker import conn
from flask_cors import CORS

DEBUG_MODE = False

app = Flask(__name__)
CORS(app)
q = Queue('main', connection=conn)

@app.route("/")
def handleQuery():
    query = request.args.get('query')
    print("API receive", query)
    if query is None:
        return "please add a 'query' parameter"

    client = pymongo.MongoClient(credentials.MONGO_URI)
    db = client.hashTrend
    queries = db.queries
    db_entry = queries.find_one({'query': query}, {'_id': False})

    if db_entry is not None:
        #print(db_entry)
        return jsonify(db_entry), 200
    else:
        query_db = {
            "query": query,
            "status": "Queuing job...",
            "code": 202,
            "result": {},
            "date": datetime.datetime.utcnow()
        }
        ret = query_db.copy()
        queries.insert_one(query_db)
        job = q.enqueue(make_report, args=(query,), job_timeout='10m')

        return ret, 200

# For testing only
@app.route("/db")
def handleDB():
    client = pymongo.MongoClient(credentials.MONGO_URI)
    db = client.hashTrend
    queries = db.queries
    db_entries = queries.find({}, {'_id': False})
    return str(list(db_entries))

# For testing only
@app.route("/delete")
def deleteDB():
    client = pymongo.MongoClient(credentials.MONGO_URI)
    db = client.hashTrend
    queries = db.queries
    queries.drop()
    return str("Done")

if __name__ == "__main__":
    if DEBUG_MODE:
        app.run(host="127.0.0.1", port=8080, debug=True)
    else:
        #app.run(host="0.0.0.0", port=80, debug=True)
        serve(app, host="0.0.0.0", port=80)
