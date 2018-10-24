import click
import jieba
import jieba.analyse


from settings import get_db, collections

db = get_db()

def handler_jieba_text_rank():
    collection = db['article-juejin']

    jieba.analyse.set_stop_words('stopword.txt')

    for data in collection.find():
        body = data['body']
        doc_id = data['document_id']
        ranks = []
        for x, w in jieba.analyse.textrank(body, topK=30,
            withWeight=True, allowPOS=('ns', 'n')):
            ranks.append({x: w})
        data = {
            '$set': {
                'jieba_text_rank': ranks
            }
        }
        collection.update_one({'document_id': doc_id}, data, upsert=True)


@click.command()
@click.option('--collection', '-c', type=click.Choice(collections), multiple=True)
def textrank(collection):
    h = collection if collection else collections
    print(h)
    if 'jieba' in h:
        handler_jieba_text_rank()