import os

from pymongo import MongoClient

host = os.environ.get('MONGO_INITDB_HOST', 'localhost')
port = os.environ.get('MONGO_INITDB_PORT', 27017)
user = os.environ.get('MONGO_INITDB_ROOT_USERNAME', 'root')
password = os.environ.get('MONGO_INITDB_ROOT_PASSWORD', 'Song123654')


def get_db():
    client = MongoClient(host, port, username=user, password=password)
    db = client.data
    return db


RABBITMQ_URL = os.environ.get('RABBITMQ_URL', 'amqp://guest:guest@localhost:5672/%2F')

collections = ['article-cnblogs', 'article-juejin', 'article-csdn']
