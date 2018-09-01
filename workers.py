import click
import logging
import jieba
import json
import math

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
        collection.update_one(condition, {'$set': doc})
        logger.info('Segmenting: {}'.format(doc_id))

    if doc.get('after_tf', None) is not None:
        logger.warning('{}--{} already have tf'.format(collection_name, doc_id))
        return

    logger.info('to calculate tf: {}--{}'.format(collection_name, doc_id))
    message = {
        'collection_name': collection_name,
        'doc_id': doc_id,
    }
    publish(
        exchange_name='data_analysis',
        body=message,
        routing_key='calculate_tf',
    )


def calculate_tf(collection_name, doc_id):
    condition = {'document_id': doc_id}
    collection, doc = find_infos(collection_name, condition)

    words = doc.get('words', None)
    if words is None:
        return

    word_list = [word for word in words.split() if check_word(word)]
    word_dict = defaultdict(int)
    for word in word_list:
        word_dict[word] += 1

    tf_collection = db['TF']
    requests = []
    for word, count in word_dict.items():
        word_md5 = md5(word)
        tf_condition = {'word_md5': word_md5}
        tf = tf_collection.find_one(tf_condition) or {'word': word, 'word_md5': word_md5}
        if tf.get(doc_id, None):
            continue
        tf.update({
            doc_id: count,
        })
        requests.append(UpdateOne(tf_condition, {'$set': tf}, upsert=True))
    if requests:
        tf_collection.bulk_write(requests)
    collection.update_one(condition, {'$set': {'after_tf': 1}}, upsert=True)
    logger.info('Calculating TF: {}--{}'.format(collection_name, doc_id))


def calculate_idf(collection_name, word):
    condition = {'word': word}
    collection, doc = find_infos(collection_name, condition)

    if doc.get('idf', None):
        doc.pop('idf')

    count = len(doc) - 2 + 1
    collection.update_one(condition, {'$set': {'idf': math.log(all_count / count)}}, upsert=True)
    logger.info('Calculating IDF: {}'.format(word))


def calculate_tf_idf(collection_name, doc_id):
    condition = {'document_id': doc_id}
    collection, doc = find_infos(collection_name, condition)

    words = doc.get('words', None)
    if words is None:
        return

    word_list = [word for word in words.split() if check_word(word)]
    word_count = len(word_list)
    word_list = list(set(word_list))
    tf_collection = db['TF']
    tf_idf_doc = {'index': collection_name}
    for word in word_list:
        tf = tf_collection.find_one({'word': word}) or {}
        tf_idf = tf.get(doc_id, 0) * tf.get('idf', 0) / word_count
        tf_idf_doc.update({
            word: tf_idf,
        })

    db['TF_IDF'].update_one(condition, {'$set': tf_idf_doc}, upsert=True)
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
        for doc in db[collection].find():
            if action == 'calculate_idf':
                message = {
                    'collection_name': collection,
                    'word': doc['word'],
                }
                logger.info('{}----{}'.format(collection, doc['word']))
            else:
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


action_map_func = {
    'segment': segment_word,
    'calculate_tf': calculate_tf,
    'calculate_idf': calculate_idf,
    'calculate_tf_idf': calculate_tf_idf,
}


def on_message_func(ch, method, properties, body):
    body = json.loads(body)
    action_map_func[method.routing_key](**body)
    ch.basic_ack(delivery_tag=method.delivery_tag)


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
