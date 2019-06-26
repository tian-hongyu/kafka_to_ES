from kafka import KafkaProducer
import json, logging, zmq, kafka

logging.basicConfig(level=logging.INFO,
                    filename='./kafka.log',
                    filemode='a',
                    format=
                    '%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s'
                    )
logger = logging.getLogger(__name__)

try:
    ctx = zmq.Context()
    s = ctx.socket(zmq.SUB)
    producer = KafkaProducer(bootstrap_servers=['192.168.10.102:9092'])

except kafka.errors.NoBrokersAvailable as e:
    print e
    logger.info(e)


def parse_parama(msg_dict):
    name_dict = {}
    try:
        for k, v in msg_dict.items():
            if k == "57590":
                name_dict["L7_PROTO"] = v
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
                name_dict["UPSTREAM_TUNNEL_ID"] = v
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
        logger.info(e)


def producer_to_kafka():
    s.connect("tcp://192.168.10.79:5556")
    s.setsockopt(zmq.SUBSCRIBE, b'flow')
    while True:
        msg = s.recv()
        print msg
        if msg[:2] == b'{"':
            try:
                msg_dict = json.loads(msg)
            except ValueError as e:
                logger.error(e)
                if msg_dict["57590"] and msg_dict["57590"][0] == "5" or msg_dict["57590"][0] == "7":
                    name_dict = parse_parama(msg_dict)
                    producer.send('dns', bytes(name_dict.encode(encoding='utf-8')))
                    producer.flush()


                # if msg_dict["57590"] and msg_dict["57590"][0] == "7":
                #     name_dict = parse_parama(msg_dict)
                #     producer.send('dns', bytes(name_dict.encode(encoding='utf-8')))
                #     producer.flush()
                else:
                    pass

            except Exception as e:
                print e


def run():
    producer_to_kafka()


if __name__ == '__main__':
    run()
