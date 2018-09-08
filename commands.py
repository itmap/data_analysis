import click
import logging
import jieba

from settings import collections as raw_collections
from tf_idf import TFIDF

logging.basicConfig(level=logging.INFO)
jieba.setLogLevel(logging.INFO)

logger = logging.getLogger(__name__)

counts = {
    'article-juejin': 4981,
    'article-cnblogs': 28618,
    'article-csdn': 275742,
}


@click.command()
@click.option('--segment', '-s', is_flag=True, help='分词')
@click.option('--idf', '-i', is_flag=True, help='计算IDF')
@click.option('--tfidf', '-t', is_flag=True, help='计算TF*IDF')
@click.argument('collections', type=click.Choice(raw_collections), nargs=-1)
def analysis_data(segment, idf, tfidf, collections):
    """
    输入mongo中的collection名

    对输入的数据集做分词、计算TF、计算IDF、计算TF*IDF操作
    """
    all_count = sum(counts[c] for c in collections)
    logger.info('calculate {}.'.format(collections))
    tfidf_obj = TFIDF(
        collection_names=collections,
        all_count=all_count,
    )
    if segment:
        tfidf_obj.generate_segment_requests_using_pool()
#        tfidf_obj.generate_segment_requests()
    if idf:
        tfidf_obj.generate_idf_requests()
    if tfidf:
        tfidf_obj.generate_tf_idf_requests()


if __name__ == '__main__':
    analysis_data()
