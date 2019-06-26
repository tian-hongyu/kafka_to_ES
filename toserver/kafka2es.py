from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
import json

try:

    consumer_dns = KafkaConsumer('dns', bootstrap_servers=['192.168.10.102:9092'])
    consumer_http = KafkaConsumer('http', bootstrap_servers=['192.168.10.102:9092'])
    es = Elasticsearch(["192.168.10.101"], port=9200)
    es.indices.create(index='dns', ignore=400)
    es.indices.create(index='http', ignore=400)
except Exception as e:
    print e


def consumer_from_kafka():
    dns_actions = []
    http_actions = []
    for message in consumer_dns:
        try:
            to_es_body = json.loads(message.value)
            print to_es_body
            if to_es_body["L7_PROTO"][0] == u"5":
                dns_actions.append(to_es_body)
                if len(dns_actions) == 50:
                    es.bulk(index="dns", doc_type="dns-type", body=dns_actions)
                    dns_actions = []
                continue
                # es.index(index="dns", doc_type="dns-type", body=to_es_body)
            else:
                http_actions.append(to_es_body)
                if len(http_actions) == 50:
                    es.bulk(index="http",doc_type="http-type",body=http_actions)
                    http_actions = []
                continue
                # es.index(index="http", doc_type="http-type", body=to_es_body)
        except Exception as e:
            print e



def run():
    consumer_from_kafka()


if __name__ == '__main__':
    run()
