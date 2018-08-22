import click
import jieba

from settings import db
from stopwords import stopwords

handlers = ['jieba']


def handler_jieba_word_segmentation():
    collection = db['article-juejin']
    #移除停用词

    for data in collection.find():
        body = data['body']
        doc_id = data['document_id']
        segs = jieba.cut(body)
        data = {
            '$set': {
                'jieba_word_segmentation': list(set([s for s in segs
                    if s not in stopwords.stopword.values and len(s.strip())>0]))
            }
        }
        collection.update_one({'document_id': doc_id}, data, upsert=True)

@click.command()
@click.option('--handler', '-h', type=click.Choice(handlers), multiple=True)
def segmentation(handler):
    h = handler if handler else handlers
    if 'jieba' in h:
        handler_jieba_word_segmentation()
