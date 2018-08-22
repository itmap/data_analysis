import click
import logging
import jieba
import math

from collections import defaultdict
from decimal import Decimal
from settings import db
from stopwords import stopwords

from pub_sub import publish

values = stopwords.stopword.values
juejin_count = 4981
cnblogs_count = 28618
csdn_count = 275742
all_count = juejin_count + csdn_count + cnblogs_count

logger = logging.getLogger(__name__)


def check_word(word):
    return len(word.strip()) > 0


def find_infos(collection_name, condition):
    collection = db[collection_name]

    doc = collection.find_one(condition)
    return collection, doc


def word_segmentation(collection_name, doc_id):
    condition = {'document_id': doc_id}
    collection, doc = find_infos(collection_name, condition)

    if doc.get('words', None):
        doc.pop('words')

    body = doc['body']
    doc['words'] = ' '.join([s for s in jieba.cut(body) if s not in values and check_word(s)])
    collection.update_one(condition, {'$set': doc})
    logger.info('Segmenting: {}'.format(doc_id))
    message = {
        'collection_name': collection_name,
        'doc_id': doc_id,
    }
    publish(
        exchange_name='data_analysis',
        msg=message,
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
        word_dict['word'] += 1

    tf_collection = db['TF']
    for word, count in word_dict.items():
        condition = {'word': word}
        tf = tf_collection.find_one(condition) or {}
        tf.update({
            doc_id: count,
        })
        tf_collection.update_one(condition, {'$set': tf}, upsert=True)
    logger.info('Calculating TF: {}'.format(doc_id))


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

    word_list = list(set([word for word in words.split() if check_word(word)]))
    tf_collection = db['TF']
    tf_idf_doc = {'index': collection_name}
    for word in word_list:
        tf = tf_collection.find_one({'word': word}) or {}
        tf_idf = tf.get(doc_id, 0) * tf.get('idf', 0)
        tf_idf_doc.update({
            word: tf_idf,
        })

    db['TF_IDF'].update_one(condition, {'$set': tf_idf_doc}, upsert=True)
    logger.info('Calculating TF*IDF: {}'.format(doc_id))


def get_first_n_word(collection_name, doc_id, limit=3):
    condition = {'document_id': doc_id}
    collection, doc = find_infos(collection_name, condition)

    for key in ('document_id', '_id', 'index'):
        doc.pop(key)
    result = [(k,doc[k]) for k in sorted(doc.values(), reversed=True)][:limit]
    return result


collections = ['article-cnblogs', 'article-juejin', 'article-csdn']
actions = ['segment', 'calculate_idf', 'calculate_tf_idf']

@click.command()
@click.option('--collection', type=click.Choice(collections), help='mongo中的collection名')
@click.option('--action', type=click.Choice(actions), help='可执行动作')
def entrance(collection, action):
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
            else:
                message = {
                    'collection_name': collection,
                    'doc_id': doc['document_id'],
                }
            publish(
                exchange_name='data_analysis',
                msg=message,
                routing_key=action,
            )

