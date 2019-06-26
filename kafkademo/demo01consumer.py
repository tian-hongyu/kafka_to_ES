from kafka import KafkaConsumer, KafkaProducer, KafkaClient
import time


def pro():
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
    for i in range(3):
        msg = "msg%d" % i
        producer.send('you', bytes(msg.encode(encoding='utf-8')))
        producer.close()


def con():
    consumer = KafkaConsumer('you', bootstrap_servers=['localhost:9092'])
    for msg in consumer:
        recv = "%s:%d:%d: key=%s value=%s" % (msg.topic, msg.partition, msg.offset, msg.key, msg.value)
        print(recv)

        time.sleep(1)


def run():
    pro()
    time.sleep(2)
    con()


if __name__ == '__main__':
    run()
