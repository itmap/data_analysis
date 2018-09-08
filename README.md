# DATA ANALYSIS


## Run
```
docker-compose up -d
docker配置尽量最大，要小心内存不够
```

## Order
分词 --> 计算TF --> 计算IDF --> 计算TF_IDF
目前的处理逻辑主要在tf_idf.py中。


## Architecture(DEPRECATED)
不再使用消息队列，直接通过多进程加速。
![architecture](https://github.com/itmap/data_analysis/blob/master/arch.png)


## Speed
4核（2.7G i5） 8G配置：30w documents，2~3 hours


## Help
python commands.py --help
