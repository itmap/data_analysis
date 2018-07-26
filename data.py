import pymongo
import json
import os

from pymongo import MongoClient


user = os.environ.get('MONGO_INITDB_ROOT_USERNAME', 'root')
password = os.environ.get('MONGO_INITDB_ROOT_PASSWORD', 'root')
client = MongoClient('mongo', 27017, username=user, password=password)
db = client.data
files = ['juejin.log']
for fl in files:
    with open(fl, 'r') as f:
        line = f.readline()
        while line:
            data = json.loads(line)
            index = data['index']
            table = db[index]
            doc_id = data['document_id']
            table.update({'document_id': doc_id}, data, upsert=True)
            line = f.readline()
