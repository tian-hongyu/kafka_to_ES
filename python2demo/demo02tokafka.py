from kafka import KafkaProducer
import json, logging, zmq, kafka

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
            elif k == "57591":
                name_dict["L7_PROTO_NAME"] = v
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
            elif k == "57578":
                name_dict["UPSTREAM_TUNNEL_ID "] = v
            elif k == "57592":
                name_dict["DOWNSTREAM_TUNNEL_ID"] = v
            elif k == "57677":
                name_dict["DNS_QUERY"] = v
            elif k == "57832":
                name_dict["HTTP_METHOD"] = v
            elif k == "57652":
                name_dict["HTTP_URL"] = v
            else:
                pass
        return json.dumps(name_dict)
    except Exception as e:
        print e


def producer_to_kafka():
    dns_proto = "dns"
    http_proto = "http"
    for i in range(1, 5):

        msg_dict = {"57590": "5.126" + i * "**", "57591": "DNS.Google", "8": "114.114.114.114", "12": "192.168.0.2",
                    "7": 53,
                    "11": 49307, "57592": 1, "57578": 1, "57943": "192.168.10.79"}

        try:
            if dns_proto in msg_dict["57591"].lower():
                name_dict = parse_parama(msg_dict)
                producer.send('dns', bytes(name_dict.encode(encoding='utf-8')))
                producer.flush()


            elif  msg_dict["57652"]:
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
