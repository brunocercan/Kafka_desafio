import pymysql
from app import app
import pymysql.cursors
from flask import json, jsonify
from flask import request
from flaskext.mysql import MySQL
from kafka import KafkaProducer
from kafka.errors import KafkaError

mysql = MySQL()
mysql.init_app(app)
#from configdb import *

############################# COMANDOS MYSQL
MYSQL_GET_CLIENTE_ESPECIFICO = 'select nome as Nome from dbclientes.clientes where idCliente = %s'
MYSQL_GET_ALL = 'select idCliente as IDCliente, nome as Nome from dbclientes.clientes'
#############################

def conecta_banco():
    connection = pymysql.connect(db='dbclientes', user='root', password='', host='192.168.0.9', port=3306)
    return connection

def get_all_clientes():
    connection = conecta_banco()
    with connection.cursor() as cur:
        cur.execute(MYSQL_GET_ALL)
        res = [dict((cur.description[i][0], value) for i, value in enumerate(row)) for row in cur.fetchall()]
        clientes = {
            'Clientes': res
        }
    return clientes

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

Clientes = get_all_clientes()
for cliente in Clientes['Clientes']:
    user_encode_data = json.dumps(cliente).encode('utf-8')
    future = producer.send('CLIENTES', user_encode_data)
    try:
        record_metadata = future.get(timeout=10)
    except KafkaError:
        # Decide what to do if produce request failed...
        log.exception()
        pass
    print (record_metadata.topic)
    print (record_metadata.partition)
    print (record_metadata.offset)