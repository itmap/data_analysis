import click
import logging
import jieba
import math
import os
import pymongo
import time

from collections import defaultdict
from functools import wraps
from multiprocessing import Process, Manager, Pool
from pymongo.operations import UpdateOne
from settings import get_db
from tqdm import tqdm
from werkzeug.utils import cached_property

logger = logging.getLogger(__name__)

with open('stopword.txt', 'r') as f:
    words = f.readlines()
stopwords = set([s.strip() for s in words])

manager = Manager()
db = get_db()


def check_word(word):
    word = word.strip()
    return word not in stopwords and 0 < len(word) < 100


def cost(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        r = func(*args, **kwargs)
        logger.info('{} cost {:.2f}s.'.format(func.__name__, time.time() - start))
        return r
    return wrapper


class TFIDF:
    def __init__(self, collection_names, all_count, tf_name='TF', idf_name='IDF'):
        self.collection_names = collection_names
        self.all_count = all_count
        self.tf_name = tf_name
        self.idf_name = idf_name
        self.cpu_count = os.cpu_count()
        self.create_collection_index()
        self.init_manager()

    def create_collection_index(self):
        db[self.tf_name].create_index([('document_id', pymongo.ASCENDING), ('word', pymongo.ASCENDING)], unique=True)
        db[self.idf_name].create_index([('word', pymongo.ASCENDING)], unique=True)

    def init_manager(self):
        self.manager_segment_requests = manager.list()
        self.manager_tf_requests = manager.list()

    def execute_mongo_crud(self, c_name, requests, exec_type, position=None, step=10000):
        """执行mongo CRUD操作"""
        method_map = {
            'update': 'bulk_write',
            'insert': 'insert_many',
        }
#        desc = 'Process {}: exec_mongo_crud'.format(os.getpid())
#        with tqdm(range(0, len(requests), step), desc=desc, ascii=True, position=position, mininterval=1.0) as bar:
#            for i in bar:
        for i in range(0, len(requests), step):
            self.execute(get_db()[c_name], method_map[exec_type], requests[i: i + step])
        del requests

    @staticmethod
    def execute(collection, method, data):
        return getattr(collection, method)(data)

    def generate_segment_requests_using_pool(self):
        """使用进程池生成分词"""
        logger.info('----STEP: SEGMENT----')
        for c_name in self.collection_names:
            return_values = {'_id': 0, 'document_id': 1, 'body': 1}
            docs = [doc for doc in db[c_name].find({}, return_values)]
            length = len(docs)
            step = int(length / self.cpu_count) + 1
            step = min(step, 10000)
            p = Pool(self.cpu_count)
            split_docs = []
            for index, i in enumerate(range(0, length, step)):
                split_docs.append((index, c_name, docs[i: i + step]))
#                p.apply_async(self.segment, args=(docs[i: i + step],))
            p.map(self.segment, split_docs)
            p.close()
            p.join()
            del docs
            del split_docs
            print('\n' * (self.cpu_count + 1))

#        processes = []
#        for i in range(0, length, step):
#            t = Process(target=self.segment, args=(docs[i: i + step],))
#            t.daemon = True
#            processes.append(t)
#        for t in processes:
#            t.start()
#        for t in processes:
#            t.join()
#            self.execute_mongo_crud(c_name, self.manager_segment_requests, 'update')
#            self.execute_mongo_crud(self.tf_name, self.manager_tf_requests, 'insert')
#            self.init_manager()
#
    def segment(self, args):
        index, c_name, docs = args
        desc = 'Process {}: segment'.format(os.getpid())
        segment_requests, tf_requests = [], []
        with tqdm(docs, desc=desc, ascii=True, position=index, mininterval=1.0) as bar:
            for doc in bar:
                word_list = [s for s in jieba.cut(doc['body']) if check_word(s)]
                words = ' '.join(word_list)
                segment_requests.append(UpdateOne(
                    {'document_id': doc['document_id']},
                    {'$set': {'words': words}},
                    upsert=True
                ))
                words_tf = self.calculate_words_tf(word_list)
                for word, tf in words_tf.items():
                    tf_requests.append({
                        'document_id': doc['document_id'],
                        'word': word,
                        'tf': tf,
                    })
#            self.manager_segment_requests.extend(temp1)
#            self.manager_tf_requests.extend(temp2)
        self.execute_mongo_crud(c_name, segment_requests, 'update', index)
        self.execute_mongo_crud(self.tf_name, tf_requests, 'insert', index)
        del docs

    def generate_segment_requests(self):
        """生成分词"""
        logger.info('----STEP: SEGMENT----')
        for c_name in self.collection_names:
            segment_requests, tf_requests = self.assemble_segment_docs(c_name)
            self.execute_mongo_crud(c_name, segment_requests, 'update')
            self.execute_mongo_crud(self.tf_name, tf_requests, 'insert')

    def assemble_segment_docs(self, c_name):
        return_values = {'_id': 0, 'document_id': 1, 'body': 1}
        segment_requests, tf_requests = [], []
        length = db[c_name].count_documents({})
        with tqdm(db[c_name].find({}, return_values), desc='assemble_segment_docs', total=length, ascii=True, mininterval=1.0) as bar:
            for index, doc in enumerate(bar):
                word_list = [s for s in jieba.cut(doc['body']) if check_word(s)]
                words = ' '.join(word_list)
                segment_requests.append(UpdateOne(
                    {'document_id': doc['document_id']},
                    {'$set': {'words': words}},
                    upsert=True
                ))
                words_tf = self.calculate_words_tf(word_list)
                for word, tf in words_tf.items():
                    tf_requests.append({
                        'document_id': doc['document_id'],
                        'word': word,
                        'tf': tf,
                    })

        return segment_requests, tf_requests

    def calculate_words_tf(self, words):
        """计算一篇文章中的TF"""
        words_tf = defaultdict(int)
        for word in words:
            words_tf[word] += 1
        return words_tf

    def generate_idf_requests(self):
        """计算IDF"""
        logger.info('----STEP: IDF----')
        word_count = defaultdict(int)
        return_values = {'_id': 0, 'word': 1}
        length = db[self.tf_name].count_documents({})
        with tqdm(db[self.tf_name].find({}, return_values), desc='calculate_word_count', total=length, ascii=True, mininterval=1.0) as bar:
            for word_tf in bar:
                word_count[word_tf['word']] += 1

        requests = []
        for word, count in word_count.items():
            requests.append({
                'word': word,
                'idf': self.calculate_idf(self.all_count, count),
            })

        self.execute_mongo_crud(self.idf_name, requests, 'insert')

    @staticmethod
    def calculate_idf(all_count, count):
        """按照算法，返回idf的值"""
        return math.log(all_count / count)

    def generate_tf_idf_requests(self):
        """计算TF*IDF"""
        logger.info('----STEP: TF*IDF----')
        for c_name in self.collection_names:
            self.assemble_tf_idf_docs(c_name)

    def assemble_tf_idf_docs(self, c_name):
        requests = []
        return_values = {'_id': 0, 'words': 1, 'document_id': 1}
        length = db[c_name].count_documents({})
        with tqdm(db[c_name].find({}, return_values), desc='assemble_tf_idf_docs', total=length, ascii=True, mininterval=1.0) as bar:
            for index, doc in enumerate(bar):
                doc_id = doc['document_id']

                word_tf_idf = self.calculate_tf_idf(doc_id, doc['words'])
                requests.append(UpdateOne(
                    {'document_id': doc_id},
                    {'$set': {'word_tf_idf': word_tf_idf}},
                    upsert=True,
                ))
                if len(requests) == 10000 or index == length - 1:
                    self.execute_mongo_crud(c_name, requests, 'update')
                    requests = []

    @cached_property
    def idf_dict(self):
        return_values = {'_id': 0}
        d = {}
        for idf in db[self.idf_name].find({}, return_values):
            d[idf['word']] = idf['idf']
        return d

    def calculate_tf_idf(self, doc_id, words):
        word_list = [word for word in words.split()]
        word_count = len(word_list)
        word_tf_idf = []

        tf_condition = {'document_id': doc_id}
        tf_return_values = {'_id': 0, 'word': 1, 'tf': 1}
        for tf in db[self.tf_name].find(tf_condition, tf_return_values):
            word = tf['word']
            tf_idf = tf['tf'] * self.idf_dict.get('word', 0) / word_count
            word_tf_idf.append(
                (word, tf_idf),
            )

        word_tf_idf.sort(key=lambda r: r[1], reverse=True)
        return word_tf_idf
