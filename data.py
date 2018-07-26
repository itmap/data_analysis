import pymongo
import json

from pymongo import MongoClient

client = MongoClient('mongo',27017)
db = client.data

with open('/home/ubuntu/workspace/spiders/logs/juejin/info.log', 'r') as f:
    line = f.readline()
    while line:
        data = json.loads(line)
        index = data['index']
        table = db[index]
        doc_id = data['document_id']
        table.update({'document_id':doc_id},data,{'upsert': True})
        line = f.readline()
