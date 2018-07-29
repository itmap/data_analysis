import json

from settings import db


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
