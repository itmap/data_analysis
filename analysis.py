import pandas
import jieba
import numpy

from settings import db

collection = db['article-juejin']
indexs, bodies = [], []
for data in collection.find():
    indexs.append(data['document_id'])
    bodies.append(data['body'])

corpos = pandas.DataFrame({
    'indexs': indexs, 
    'bodies': bodies
});


segments = []
indexs = []

for index, row in corpos.iterrows():
    ind = row['indexs']
    body = row['bodies']
    segs = jieba.cut(body)
    for seg in segs:
        segments.append(seg)
        indexs.append(ind)

 
segmentDataFrame = pandas.DataFrame({
    'segment': segments, 
    'indexs': indexs
})

#进行词频统计  
segStat = segmentDataFrame.groupby(
    by='segment'
)['segment'].agg({
    u'计数': numpy.size
}).reset_index().sort_values( #重新设置索引
    by=[u'计数'],
    ascending=False  #倒序排序
)

"""排在前面的为停用词"""

#移除停用词
stopwords = pandas.read_csv(
    'stopword.txt',
    encoding='utf8',
    index_col=False
)

#获得没有停用词的词频统计结果
fSegStat = segStat[
    ~segStat.segment.isin(stopwords.stopword)
]  #'~'取反，不包含停用词的留下。
