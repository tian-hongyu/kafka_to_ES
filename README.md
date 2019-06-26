# kafka
通过抓包工具nrpobe，结合Python语言以及kafka消息队列，将抓包数据存入ES
这并不是一个统一的项目；
### toserver路径下的py文件
zmq2kafka.py：在主机上运行nprobe截获的数据流量放进zmq队列，通过该脚本，从zmq中取出数据再放入到kafka队列；
kafka2es.py:从kafka消息队列中取出数据，存入ES；
get_es_data.py：测试ES中是否存有数据；

### kafkademo路径下的py文件
demo01producer.py此脚本用于测试kafka生产者
demo01consumer.py此脚本用于测试kafka消费者

