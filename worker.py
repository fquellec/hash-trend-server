import os
import pymongo
import credentials
import redis
from rq import Worker, Queue, Connection
import return_codes

listen = ['main']

redis_url = os.getenv('REDISTOGO_URL', 'redis://localhost:6379')

conn = redis.from_url(redis_url)

def my_handler(job, exc_type, exc_value, traceback):
    print("Execution Failed, updating db...") 
    client = pymongo.MongoClient(credentials.MONGO_URI)
    db = client.hashTrend
    queries = db.queries
    myquery = { "job_id": str(job.id) }
    newvalues = { "$set": { "job_id": None, "code": return_codes.ERROR, "status": ("Error " + str(exc_type) + " : " + str(exc_value)  + str(traceback))} }
    queries.update_one(myquery, newvalues)

if __name__ == '__main__':
    with Connection(conn):
        worker = Worker(list(map(Queue, listen)), exception_handlers=[my_handler], disable_default_exception_handler=True)
        worker.work()
