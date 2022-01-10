from flask import json, jsonify
from flask import request
from app import app
from kafka import KafkaConsumer



KafkaConsumer(auto_offset_reset='earliest', enable_auto_commit=True)

consumer = KafkaConsumer(group_id='console-consumer-27716', bootstrap_servers=['localhost:9092'])
consumer.subscribe('CLIENTES')

for message in consumer:
    message_str = message.value.decode('utf-8')
    consumer.commit()
    print(message_str)