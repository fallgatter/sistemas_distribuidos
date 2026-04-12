import json
import pika
import os
from Crypto.Signature import pkcs1_15
from Crypto.Hash import SHA256
from Crypto.PublicKey import RSA

pwd = b'senha'

if os.path.exists("./private_keys/mspromocao_privatekey.pem"):
    with open("./private_keys/mspromocao_privatekey.pem", "rb") as f:
        data = f.read()
        mykey = RSA.import_key(data, pwd)

else: 
    mykey = RSA.generate(2048)
    with open("./private_keys/mspromocao_privatekey.pem", "wb") as f:
        data = mykey.export_key(passphrase=pwd,
                              pkcs=8,
                              protection='PBKDF2WithHMAC-SHA512AndAES256-CBC',
                              prot_params={'iteration_count':131072})
        f.write(data)

    with open("./public_keys/mspromocao_publickey.pem", "wb") as f:
        public_key = mykey.publickey()
        data = public_key.export_key()
        f.write(data)

with open("./public_keys/msgateway_publickey.pem", "rb") as f:
    data = f.read()
    gateway_pub_key = RSA.import_key(data)


def callback(ch, method, properties, body):
    print("Evento recebido")
    
    signature = properties.headers.get("signature")
    if signature is None:
        print("Evento sem assinatura")
        return
     
    h = SHA256.new(body)

    try:
        pkcs1_15.new(gateway_pub_key).verify(h, signature)
        print("Assinatura valida")

    except (ValueError, TypeError):
        print("Assinatura invalida")
        return

    try:
        new_signature = pkcs1_15.new(mykey).sign(h)

        channel.basic_publish(
            exchange='Promocoes', 
            routing_key='promocao.publicada', 
            body=body, 
            properties=pika.BasicProperties(
                headers={'signature': new_signature
                }
            )
        )
        print("Promocao publicada")

    except Exception as e:
        print("erro ao publicar", e)
    

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
channel.exchange_declare(exchange='Promocoes', exchange_type='topic')

result = channel.queue_declare('fila_promocao', exclusive=True)
queue_name = result.method.queue

channel.queue_bind(exchange='Promocoes', queue=queue_name, routing_key='promocao.recebida')

channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
    
try:
    channel.start_consuming()
except KeyboardInterrupt:
    connection.close()
