import click
import jieba

from settings import db, collections
from stopwords import stopwords


def handler_jieba_word_segmentation():
    collection = db['article-juejin']
    #移除停用词

    for data in collection.find():
        body = data['body']
        doc_id = data['document_id']
        segs = jieba.cut(body)
        data = {
            '$set': {
                'jieba_word_segmentation': list([s for s in segs
                    if s not in stopwords.stopword.values and len(s.strip())>0])
            }
        }
        collection.update_one({'document_id': doc_id}, data, upsert=True)

@click.command()
@click.option('--collection', '-c', type=click.Choice(collections), multiple=True)
def segmentation(collection):
    h = collection if collection else collections
    if 'jieba' in h:
        handler_jieba_word_segmentation()
