from kafka import KafkaProducer
from elasticsearch import Elasticsearch
import json, logging
import kafka


logging.basicConfig(level=logging.DEBUG,
                    filename='./kafka.log',
                    filemode='a',
                    format=
                    '%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s'
                    )
logger = logging.getLogger(__name__)

try:
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
except kafka.errors.NoBrokersAvailable as e:
    print e


def parse_parama(msg_dict):
    name_dict = {}
    try:
        for k, v in msg_dict.items():
            if k == "57590":
                name_dict["L7_PROTO"] = v
            # elif k == "57591":
            #     name_dict["L7_PROTO_NAME"] = v
            elif k == "8":
                name_dict["IPV4_SRC_ADDR"] = v
            elif k == "12":
                name_dict["IPV4_DST_ADDR"] = v
            elif k == "7":
                name_dict["L4_SRC_PORT"] = v
            elif k == "11":
                name_dict["L4_DST_PORT"] = v
            elif k == "4":
                name_dict["PROTOCOL"] = v
            elif k == "57592":
                name_dict["DOWNSTREAM_TUNNEL_ID"] = v
            elif k == "57677":
                name_dict["DNS_QUERY"] = v
            elif k == "57943":
                name_dict["HTTP_URL"] = v
            else:
                pass
        return json.dumps(name_dict)
    except Exception as e:
        print e


def producer_to_kafka():
    dns_proto = "dns"
    http_proto = "http"
    msg_dict = {"57590": "5.126", "57591": "DNS.Google", "8": "192.168.0.2", "12": "218.30.118.6",
                "7": 61104,
                "11": 53, "4": 17, "57592": 1, "57677": "stats.g.doubleclick.net",
                "57943": "192.168.10.79"}

    try:
        if msg_dict["57590"] and msg_dict["57590"][0] == "5":
            name_dict = parse_parama(msg_dict)
            producer.send('dns', bytes(name_dict.encode(encoding='utf-8')))
            producer.flush()


        elif http_proto in msg_dict["57591"].lower():
            name_dict = parse_parama(msg_dict)
            producer.send('http', bytes(name_dict.encode(encoding='utf-8')))
            producer.flush()
        else:
            pass

    except Exception as e:
        print e


def run():
    producer_to_kafka()



if __name__ == '__main__':
    run()
