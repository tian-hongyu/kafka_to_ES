from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
import json

mappings = {
    "mappings": {
        "dns-type": {
            "properties": {
                "L7_PROTO": {
                    "type": "keyword",
                    "index": "false"
                },


            }
        },
        "http-type": {
            "properties": {
                "id": {
                    "type": "long",
                    "index": "true"
                },

            }
        }
    }
}

try:
    consumer_dns = KafkaConsumer('dns', bootstrap_servers=['localhost:9092'])
    consumer_http = KafkaConsumer('http', bootstrap_servers=['localhost:9092'])
    es = Elasticsearch(['127.0.0.1'], port=9200)

    # es.indices.delete(index='dns', ignore=[400, 404])
    es.indices.create(index='dns', body=mappings, ignore=400)
    es.indices.create(index='http', body=mappings, ignore=400)
except Exception as e:
    print e


def consumer_from_kafka():
    dns_actions = []
    for message in consumer_dns:
        to_es_body = json.loads(message.value)
        print to_es_body
        print type(to_es_body)
        print to_es_body["L7_PROTO"]
        print type(to_es_body["L7_PROTO"])
        if to_es_body["L7_PROTO"][0] == u'5':
            # print to_es_body["L7_PROTO"][0]
            dns_actions.append(to_es_body)
            es.bulk(index="dns", doc_type="dns-type", body=to_es_body)
            print dns_actions
            # es.index(index="dns", doc_type="dns-type", body=to_es_body)


def run():
    consumer_from_kafka()


if __name__ == '__main__':
    run()
