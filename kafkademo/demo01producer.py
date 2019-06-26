from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

for i in range(4):
    msg = "msg%d" % i
    producer.send('dns', bytes(msg.encode(encoding='utf-8')))
producer.close()
