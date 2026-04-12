import json
import pika
import os
from Crypto.Signature import pkcs1_15
from Crypto.Hash import SHA256
from Crypto.PublicKey import RSA

HOT_DEAL_THRESHOLD = 3

pwd = b'senha'

if os.path.exists("./private_keys/msranking_privatekey.pem"):
    with open("./private_keys/msranking_privatekey.pem", "rb") as f:
        data = f.read()
        mykey = RSA.import_key(data, pwd)

else:
    mykey = RSA.generate(2048)
    with open("./private_keys/msranking_privatekey.pem", "wb") as f:
        data = mykey.export_key(passphrase=pwd,
                              pkcs=8,
                              protection='PBKDF2WithHMAC-SHA512AndAES256-CBC',
                              prot_params={'iteration_count':131072})
        f.write(data)

    with open("./public_keys/msranking_publickey.pem", "wb") as f:
        public_key = mykey.publickey()
        data = public_key.export_key()
        f.write(data)

lista_promocoes = {}

def callback(ch, method, properties, body):
    print(f"Promoção recebida. Verificando assinatura...")
    signature = properties.headers.get("signature")
    key = RSA.import_key(open('./public_keys/msgateway_publickey.pem').read())
    h = SHA256.new(body)
    valid_signature = False
    try:
        pkcs1_15.new(key).verify(h, signature)
        valid_signature = True
        print("Assinatura válida.")
    except (ValueError, TypeError):
        print("Assinatura inválida.")
    if valid_signature:
        promocao = json.loads(body)
        if promocao['id'] in lista_promocoes:
            lista_promocoes[promocao['id']]['votos'] += 1
            if lista_promocoes[promocao['id']]['votos'] >= HOT_DEAL_THRESHOLD:
                print(f"Promoção {promocao['id']} atingiu o status de hot deal!")
                h = SHA256.new(body)
                signature = pkcs1_15.new(mykey).sign(h)
                channel.basic_publish(
                    exchange='Promocoes',
                    routing_key='promocao.destaque',
                    body=body,
                    properties=pika.BasicProperties(
                        headers={
                            "signature": signature
                        }
                    )
                )
        else:
            lista_promocoes[promocao['id']] = {
                'promocao_json': promocao,
                'votos': 1
            }
        
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
channel.exchange_declare(exchange='Promocoes', exchange_type='topic')

result = channel.queue_declare('fila_ranking', exclusive=True)
queue_name = result.method.queue

channel.queue_bind(exchange='Promocoes', queue=queue_name, routing_key='promocao.voto')

channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)

channel.start_consuming()

connection.close()