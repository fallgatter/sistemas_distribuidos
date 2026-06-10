import json
import os
import pika
from Crypto.Signature import pkcs1_15
from Crypto.Hash import SHA256
from Crypto.PublicKey import RSA
import resend

with open(".env") as f:
    for linha in f:
        linha = linha.strip()

        if not linha or linha.startswith("#"):
            continue

        chave, valor = linha.split("=", 1)
        os.environ[chave.strip()] = valor.strip()

print(os.environ["RESEND_API_KEY"])

resend.api_key = os.environ["RESEND_API_KEY"]

if os.path.exists("./public_keys/mspromocao_publickey.pem"):
    with open("./public_keys/mspromocao_publickey.pem", "rb") as f:
        data = f.read()
        promocao_pub_key = RSA.import_key(data)
else:
    print("Public Key do MS Promoção não encontrada. Execute o ms_promocao.py primeiro.")
    exit(1)

if os.path.exists("./public_keys/msranking_publickey.pem"):
    with open("./public_keys/msranking_publickey.pem", "rb") as f:
        data = f.read()
        ranking_pub_key = RSA.import_key(data)
else:
    print("Public Key do MS Ranking não encontrada. Execute o ms_ranking.py primeiro.")
    exit(1)

with open('promocao_categorias.txt', 'r', encoding='utf-8') as f:
    lista_categorias = [linha.strip() for linha in f.readlines() if linha.strip()]

def callback(ch, method, properties, body):
    print("Promoção recebida. Verificando assinatura...")

    signature = properties.headers.get("signature")
    if signature is None:
        print("Evento sem assinatura.")
        return
    
    routing_key = method.routing_key
    if routing_key == 'promocao.publicada':
        pub_key = promocao_pub_key
    elif routing_key == 'promocao.destaque':
        pub_key = ranking_pub_key
    else:
        print("Evento com routing key desconhecida.")
        return

    h = SHA256.new(body)

    try:
        pkcs1_15.new(pub_key).verify(h, signature)
        print("Assinatura válida.")

    except (ValueError, TypeError):
        print("Assinatura inválida.")
        return

    try:
        promocao = json.loads(body)
        categoria = str(promocao.get("category")).strip()
        indice=-1

        for i, linha in enumerate(lista_categorias):
            if linha == categoria:
                indice = i
                break

        if indice != -1:
            new_routing_key = f"promocao.categoria{indice}"

            if routing_key == "promocao.destaque":
                if not promocao['title'].startswith("HOT DEAL:"):
                    promocao["title"] = f"HOT DEAL: {promocao['title']}"
                print("HOT DEAL recebido!")
                print(promocao['title'])
                resend.Emails.send({
                "from": "onboarding@resend.dev",
                "to": "leticiaforte@alunos.utfpr.edu.br",
                "subject": f"Sua promoção {promocao['title']} entrou em destaque!",
                "html": "<p>Parabéns! Sua promoção se tornou um HOT DEAL!s.</p>"
                })
                print("Email de HOT DEAL enviado via Resend.")
            else:
                resend.Emails.send({
                "from": "onboarding@resend.dev",
                "to": "leticiaforte@alunos.utfpr.edu.br",
                "subject": f"Sua promoção {promocao['title']} foi aprovada!",
                "html": "<p>Sua promoção foi aprovada e está disponível para visualização.</p>"
                })
                print("Email de promoção aprovada enviado via Resend.")

            new_body = json.dumps(promocao)
        
            channel.basic_publish(
                exchange='Promocoes', 
                routing_key=new_routing_key, 
                body=new_body
            )
            print(f"Evento enviado para {new_routing_key}.")
        else:
            print(f"Categoria '{categoria}' não mapeada.")

    except Exception as e:
        print("Erro ao publicar promoção.", e)


connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
channel.exchange_declare(exchange='Promocoes', exchange_type='topic')

result = channel.queue_declare('fila_notificacao', durable=True, exclusive=True)
queue_name = result.method.queue

channel.queue_bind(exchange='Promocoes', queue=queue_name, routing_key='promocao.publicada')
channel.queue_bind(exchange='Promocoes', queue=queue_name, routing_key='promocao.destaque')

channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
    
try:
    channel.start_consuming()
except KeyboardInterrupt:
    connection.close()

