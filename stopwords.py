import pandas

#移除停用词
stopwords = pandas.read_csv(
    'stopword.txt',
    encoding='utf8',
    index_col=False,
    sep=None,
    error_bad_lines=False,
    header=None,
    names=['stopword'],
)
