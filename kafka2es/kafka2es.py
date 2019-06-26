from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
import json




try:

    consumer_dns = KafkaConsumer('dns', bootstrap_servers=['localhost:9092'])
    consumer_http = KafkaConsumer('http', bootstrap_servers=['localhost:9092'])
    es = Elasticsearch()
    es.indices.create(index='dns', ignore=400)
    es.indices.create(index='http', ignore=400)
except Exception as e:
    print e


def consumer_from_kafka():
    for message in consumer_dns:
        to_es_body = json.loads(message.value)
        es.index(index="dns", doc_type="dns-type", body=to_es_body)
    for message in consumer_http:
        to_es_body = json.loads(message.value)
        es.index(index="dns", doc_type="http-type", body=to_es_body)


def run():
    consumer_from_kafka()


if __name__ == '__main__':
    run()
