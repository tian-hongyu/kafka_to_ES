from datetime import datetime
from elasticsearch import Elasticsearch
import json

es = Elasticsearch(["192.168.10.101"],port=9200)


def demo01():
    body = {"name": 'lucy', 'sex': 'female', 'age': 10}
    # es.index(index='index', body=body, doc_type='type', id=None)
    es.indices.create(index='my-index', ignore=400)
    es.index(index="my-index", doc_type="test-type", id=01, body=body)
    es.index(index="test-index", doc_type="test-type", id=42, body={"any": "data", "timestamp": datetime.now()})


def demo02():
    res = es.get(index="my-index", doc_type="test-type", id=01)
    print res['_source']
    # res1 = es.get(index="index", doc_type="type", id=01)
    # print res1

def demo03():
    #res = es.search(index="dns", body={"query": {"match_all": {}}})
    res = es.search(index="dns")
    #print res
    print res['_shards']['total']
    #print res['hits']['hits']
    #print res['hits']['hits'][0]['_source']
    # print res['hits']['hits'][0]
    # print res['hits']['hits']
    # print res['hits']
    # print res

def run():
    # demo01()
    # demo02()
    demo03()

if __name__ == "__main__":
    run()

