import click
import logging
import jieba
import json
import math
import pika

from collections import defaultdict
from multiprocessing import Process
from pub_sub import publish, subscribe
from pymongo.operations import UpdateOne
from settings import db, collections
# from stopwords import stopwords
from utils import md5

#values = stopwords.stopword.values
words = []
with open('stopword.txt', 'r') as f:
    words = f.readlines()
stopwords = [s.strip() for s in words]

juejin_count = 4981
cnblogs_count = 28618
csdn_count = 275742
all_count = juejin_count + csdn_count + cnblogs_count

logger = logging.getLogger(__name__)


def check_word(word):
    return 0 < len(word.strip()) < 100


def find_infos(collection_name, condition):
    collection = db[collection_name]

    doc = collection.find_one(condition)
    return collection, doc


def segment_word(collection_name, doc_id):
    condition = {'document_id': doc_id}
    collection, doc = find_infos(collection_name, condition)

    if doc.get('words', None) is None:
        body = doc['body']
        doc['words'] = ' '.join([s for s in jieba.cut(body) if s not in stopwords and check_word(s)])
        collection.update_one(condition, {'$set': doc}, upsert=True)
        logger.info('Segmenting: {}'.format(doc_id))

    if doc.get('after_tf', None) is not None:
        logger.warning('{}--{} already have tf'.format(collection_name, doc_id))
        return

#    logger.info('to calculate tf: {}--{}'.format(collection_name, doc_id))
#    message = {
#        'collection_name': collection_name,
#        'doc_id': doc_id,
#    }
#    publish(
#        exchange_name='data_analysis',
#        body=message,
#        routing_key='calculate_tf',
#    )


def calculate_tf(collection_name, doc_id=None, doc_ids=None):
    if doc_ids is None:
        return

    word_dict = defaultdict(lambda: defaultdict(int))
    requests = []
    for doc_id in doc_ids:
        condition = {'document_id': doc_id}
        collection, doc = find_infos(collection_name, condition)

        words = doc.get('words', None)
        if words is None:
            continue
        if doc.get('after_tf', None) is not None:
            logger.warning('{}--{} already have tf'.format(collection_name, doc_id))
            continue

        word_list = [word for word in words.split() if check_word(word)]
        for word in word_list:
            word_dict[word][doc_id] += 1

        requests.append(UpdateOne(condition, {'$set': {'after_tf': 1}}, upsert=True))
        logger.info('Calculating TF: {}--{}'.format(collection_name, doc_id))
    if requests:
        collection.bulk_write(requests)

    requests = []
    tf_collection = db['TF']
    for word, docs in word_dict.items():
        word_md5 = md5(word)
        tf = {
            'word': word,
            'word_md5': word_md5,
        }
        tf.update(docs)
        tf_condition = {'word_md5': word_md5}
        requests.append(UpdateOne(tf_condition, {'$set': tf}, upsert=True))
    if requests:
        tf_collection.bulk_write(requests)


def calculate_idf(collection_name, word=None, words=None):
    if words is None:
        return

    condition = {'word': {'$in': words}}
    requests = []
    for doc in db[collection_name].find(condition):
        count = len(doc) - 3 + 1
        requests.append(UpdateOne({'word': doc['word']}, {'$set': {'idf': math.log(all_count / count)}}, upsert=True))
    db[collection_name].bulk_write(requests)
    logger.info('Calculating IDF: {}~{}'.format(words[0], words[-1]))


def calculate_tf_idf(collection_name, doc_id):
    condition = {'document_id': doc_id}
    collection, doc = find_infos(collection_name, condition)

    words = doc.get('words', None)
    if words is None:
        return
    if db['TF_IDF'].find_one(condition):
        return

    word_list = [word for word in words.split() if check_word(word)]
    word_count = len(word_list)
    word_list = list(set(word_list))
    tf_collection = db['TF']
    word_tf_idf = []
    for word in word_list:
        tf = tf_collection.find_one({'word': word}) or {}
        tf_idf = tf.get(doc_id, 0) * tf.get('idf', 0) / word_count
        word_tf_idf.append(
            (word, tf_idf),
        )
    word_tf_idf.sort(key=lambda r: r[1], reverse=True)
    tf_idf_doc = {'index': collection_name, 'word_tf_idf': word_tf_idf}
    db['TF_IDF'].update_one(condition, {'$set': tf_idf_doc}, upsert=True) # insert
    logger.info('Calculating TF*IDF: {}--{}'.format(collection_name, doc_id))


def get_first_n_word(collection_name, doc_id, limit=3):
    condition = {'document_id': doc_id}
    collection, doc = find_infos(collection_name, condition)

    for key in ('document_id', '_id', 'index'):
        doc.pop(key)
    result = [(k,doc[k]) for k in sorted(doc.values(), reversed=True)][:limit]
    return result


actions = ['segment', 'calculate_tf', 'calculate_idf', 'calculate_tf_idf']

@click.command()
@click.option('--collection', type=click.Choice(collections), help='mongo中的collection名')
@click.option('--action', type=click.Choice(actions), help='可执行动作')
def entrance(collection, action):
    """开始任务"""
    if action == 'calculate_idf':
        cols = ['TF']
    else:
        cols = [collection] or collections
    for collection in cols:
        logger.info('begin {} collection: {}...'.format(action, collection))
        if action == 'segemnt':
            condition = {'words': None}
        elif action == 'calculate_tf':
            condition = {'after_tf': None}
        elif action == 'calculate_idf':
            condition = {'idf': None}
            words = []
            for index, doc in enumerate(db[collection].find(condition, {'word': 1, '_id':0})):
                if index and index % 1000 == 0:
                    message = {
                        'collection_name': collection,
                        'words': words,
                    }
                    logger.info('{}----{}~{}'.format(collection, words[0], words[-1]))
                    publish(
                        exchange_name='data_analysis',
                        body=message,
                        routing_key=action,
                    )
                    words = []
                else:
                    words.append(doc['word'])
            if words:
                message = {
                    'collection_name': collection,
                    'words': words,
                }
                logger.info('{}----{}~{}'.format(collection, words[0], words[-1]))
                publish(
                    exchange_name='data_analysis',
                    body=message,
                    routing_key=action,
                )
            return
        else:
            condition = {}
        for doc in db[collection].find(condition):
            message = {
                'collection_name': collection,
                'doc_id': doc['document_id'],
            }
            logger.info('{}----{}'.format(collection, doc['document_id']))
            publish(
                exchange_name='data_analysis',
                body=message,
                routing_key=action,
            )


@click.command()
@click.option('--collection', '-c', type=click.Choice(collections), multiple=True, help='mongo中的collection名')
def bulk_calculate_tf(collection):
    """批量计算TF"""
    condition = {'after_tf': None}
    for c in collection:
        doc_ids = []
        for index, doc in enumerate(db[c].find(condition)):
            if index and index % 1000 == 0:
                message = {
                    'collection_name': c,
                    'doc_ids': doc_ids,
                }
                logger.info('{}----total {}: {}~{}'.format(collection, len(doc_ids), doc_ids[0], doc_ids[-1]))
                publish(
                    exchange_name='data_analysis',
                    body=message,
                    routing_key='calculate_tf',
                )
                doc_ids = []
            else:
                doc_ids.append(doc['document_id'])
        if doc_ids:
                message = {
                    'collection_name': c,
                    'doc_ids': doc_ids,
                }
                logger.info('{}----total {}: {}~{}'.format(collection, len(doc_ids), doc_ids[0], doc_ids[-1]))
                publish(
                    exchange_name='data_analysis',
                    body=message,
                    routing_key='calculate_tf',
                )


action_map_func = {
    'segment': segment_word,
    'calculate_tf': calculate_tf,
    'calculate_idf': calculate_idf,
    'calculate_tf_idf': calculate_tf_idf,
}


def on_message_func(ch, method, properties, body):
    body = json.loads(body)
    action_map_func[method.routing_key](**body)
    try:
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except pika.exceptions.ConnectionClosed as e:
        logger.exception(e)


@click.command()
@click.option('--action', type=click.Choice(actions), help='可执行动作')
def start_rabbit_workers(action):
    """启动兔子工人"""

    def run_rabbit(cfg):
        subscribe(**cfg)

    pool = []
    try:
        total_actions = [action] or actions
        for action in total_actions:
            config = {
                'exchange_name': 'data_analysis',
                'exchange_type': 'direct',
                'queue_name': action,
                'routing_key': action,
                'func': on_message_func,
                'exclusive': False,
                'durable': True,
                'prefetch_count': 1,
                'no_ack': False,
            }
            #p = Process(group=None, target=run_rabbit, args=(config,))
            #p.start()
            #pool.append(p)
            run_rabbit(config)
            logger.info('starting {}...'.format(action))
        #for p in pool:
        #    p.join()
    except KeyboardInterrupt:
        pass
