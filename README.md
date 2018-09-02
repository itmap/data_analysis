# DATA ANALYSIS

## Architecture
![architecture](https://github.com/itmap/data_analysis/blob/master/arch.png)

## Run
```
docker-compose up -d
```

## Scale
```
tf_worker as example:
docker-compose scale tf_worker=2
```

## Order
详见上图，分词 --> 计算TF --> 计算IDF --> 计算TF_IDF
具体哪个docker执行详见docker-compose.yaml文件

## Speed
分词步骤速度还可以忍受。
计算TF步骤完全无法忍受，若一次只计算一篇文章的TF会非常慢。
现在一次会计算100篇文章，同时使用bulk_write来提交给mongo。还是有点慢。
目前全部文章30w不到，执行完要还是需要1day左右。
同时，若处理的worker过多的话，会报`pika.exceptions.ConnectionClosed`，暂时没有去深究。

2018-09-02：现在还在计算TF中，下面两步是否要优化一下需要看具体执行情况。


