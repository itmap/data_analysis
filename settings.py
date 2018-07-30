import os

from pymongo import MongoClient

user = os.environ.get('MONGO_INITDB_ROOT_USERNAME', 'root')
password = os.environ.get('MONGO_INITDB_ROOT_PASSWORD', 'Song123654')
client = MongoClient('mongo', 27017, username=user, password=password)
db = client.data