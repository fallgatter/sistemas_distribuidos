import json
import pika
from Crypto.Signature import pkcs1_15
from Crypto.Hash import SHA256
from Crypto.PublicKey import RSA

with open("./public_keys/mspromocao_publickey.pem", "rb") as f:
    data = f.read()
    promocao_pub_key = RSA.import_key(data)

with open("./public_keys/msranking_publickey.pem", "rb") as f:
    data = f.read()
    ranking_pub_key = RSA.import_key(data)

with open('promocao_categorias.txt', 'r', encoding='utf-8') as f:
    lista_categorias = f.readlines()

def callback(ch, method, properties, body):
    print("Evento recebido")

    signature = properties.headers.get("signature")
    if signature is None:
        print("Evento sem assinatura")
        return
    
    routing_key = method.routing_key
    if routing_key == 'promocao.publicada':
        pub_key = promocao_pub_key
    elif routing_key == 'promocao.destaque':
        pub_key = ranking_pub_key
    else:
        print("Evento com routing key desconhecida")
        return

    h = SHA256.new(body)

    try:
        pkcs1_15.new(pub_key).verify(h, signature)
        print("Assinatura valida")

    except (ValueError, TypeError):
        print("Assinatura invalida")
        return


    try:
        promocao = json.loads(body)
        categoria = promocao.get("category")
        indice=-1

        for i, linha in enumerate(lista_categorias):
            if linha.strip().lower() == categoria.lower():
                indice = i
                break

        if indice != -1:
            new_routing_key = f"promocao.categoria{indice}"

            if routing_key == "promocao.destaque":
                promocao["title"] = f"HOT DEAL: {promocao['title']}"
                print("HOT DEAL DETECTED")
                print({promocao['title']})

            new_body = json.dumps(promocao)
        
            channel.basic_publish(
                exchange='Promocoes', 
                routing_key=new_routing_key, 
                body=new_body
            )
            print(f"Evento enviado para {new_routing_key}")
        else:
            print(f"Categoria '{categoria}' não mapeada")

    except Exception as e:
        print("erro ao publicar promoção", e)


connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel =connection.channel()
channel.exchange_declare(exchange='Promocoes', exchange_type='topic')

result =channel.queue_declare('fila_notificacao', exclusive=True)
queue_name = result.method.queue

channel.queue_bind(exchange='Promocoes', queue=queue_name, routing_key='promocao.publicada')
channel.queue_bind(exchange='Promocoes', queue=queue_name, routing_key='promocao.destaque')

channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
    
try:
    channel.start_consuming()
except KeyboardInterrupt:
    connection.close()

