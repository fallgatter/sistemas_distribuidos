import json
import pika
import threading
import os
from Crypto.Signature import pkcs1_15
from Crypto.Hash import SHA256
from Crypto.PublicKey import RSA
from flask import Flask, request
import resend

app = Flask(__name__)

pwd = b'senha'

with open(".env") as f:
    for linha in f:
        linha = linha.strip()

        if not linha or linha.startswith("#"):
            continue

        chave, valor = linha.split("=", 1)
        os.environ[chave.strip()] = valor.strip()

print(os.environ["RESEND_API_KEY"])

if os.path.exists("./private_keys/msgateway_privatekey.pem"):
    with open("./private_keys/msgateway_privatekey.pem", "rb") as f:
        data = f.read()
        mykey = RSA.import_key(data, pwd)

else: 
    mykey = RSA.generate(2048)
    with open("./private_keys/msgateway_privatekey.pem", "wb") as f:
        data = mykey.export_key(passphrase=pwd,
                              pkcs=8,
                              protection='PBKDF2WithHMAC-SHA512AndAES256-CBC',
                              prot_params={'iteration_count':131072})
        f.write(data)

    with open("./public_keys/msgateway_publickey.pem", "wb") as f:
        public_key = mykey.publickey()
        data = public_key.export_key()
        f.write(data)

if os.path.exists("./public_keys/mspromocao_publickey.pem"):
    with open("./public_keys/mspromocao_publickey.pem", "rb") as f:
        data = f.read()
        promocao_pub_key = RSA.import_key(data)
else:
    print("Public Key do MS Promoção não encontrada. Execute o ms_promocao.py primeiro.")
    exit(1)


stop_event = threading.Event()
promocoes = {}
promocao_id_counter = 0
loja_id_counter = 0
lojas = {}

# espera um json do tipo:
# ({
#     "store_name": nome,
#     "e-mail": email,
# })
@app.route('/cadastrar_loja', methods=['POST'])
def cadastrar_loja():
    loja = request.get_json()
    if not loja:
        return "Dados não presentes. Cadastro cancelado.", 400
    if not loja["store_name"]:
        return "Nome da loja vazio. Cadastro cancelado.", 400
    if not loja["e-mail"]:
        return "E-mail vazio. Cadastro cancelado.", 400
    
    loja["id"] = loja_id_counter
    lojas[loja_id_counter] = loja
    loja_id_counter += 1

    return f"Loja cadastrada com sucesso.", 201

# espera um json do tipo:
# ({
#     "category": category.strip(),
#     "item_name": item_name,
#     "price": price,
#     "title": title,
#     "description": description
# })
@app.route('/cadastrar_promocao', methods=['POST'])
def cadastrar_promocao():
    promocao = request.get_json()

    if not promocao:
        return "Dados não presentes. Cadastro cancelado.", 400

    with open('promocao_categorias.txt', 'r', encoding='utf-8') as f:
        categorys = f.readlines()
    try:
        promocao["category"] = categorys[int(promocao["category"]) - 1]
    except (ValueError, IndexError):
        return "Categoria inválida. Cadastro cancelado.", 400
    
    if not promocao["item_name"]:
        return "Nome do item vazio. Cadastro cancelado.", 400

    if not promocao["title"]:
        return "Título vazio. Cadastro cancelado.", 400

    if not promocao["description"]:
        return "Descrição vazia. Cadastro cancelado.", 400

    try:
        float(promocao["price"])
    except ValueError:
        return "Valor inválido. Cadastro cancelado.", 400

    routing_key = "promocao.recebida"
    
    promocao["id"] = promocao_id_counter

    menu_connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    menu_channel = menu_connection.channel()
    menu_channel.exchange_declare(exchange='Promocoes', exchange_type='topic')

    promocao_id_counter += 1

    h = SHA256.new(json.dumps(promocao).encode())
    signature = pkcs1_15.new(mykey).sign(h)

    menu_channel.basic_publish(
        exchange='Promocoes',
        routing_key=routing_key,
        body=json.dumps(promocao),
        properties=pika.BasicProperties(
            headers={
                "signature": signature
            }
        )
    )

    menu_connection.close()
    return "Promoção cadastrada com sucesso.", 201

@app.route('/listar_promocoes', methods=['GET'])
def get_promocoes():
    if len(promocoes) == 0:
        return "Nenhuma promoção disponível.", 200
    else:
        return json.dumps(promocoes), 200

# espera um json do tipo:
# ({
#     "id": id
#     "voto": 1 ou 0
# })
@app.route('/votar', methods=['POST'])
def votar():
    promocao = request.get_json()
    if len(promocoes) == 0:
        return "Nenhuma promoção disponível.", 200
    else:   
        if promocao["id"] not in promocoes:
            return "ID inválido. Voto não registrado.", 400

        if promocao["voto"] not in [0, 1]:
            return "Voto inválido. Voto não registrado.", 400
        if promocao["voto"] == 1:
            vote = "upvote"
        else:
            vote = "downvote"

        routing_key = "promocao.voto"
        body = json.dumps(promocoes[promocao["id"]])

        h = SHA256.new(body.encode())
        signature = pkcs1_15.new(mykey).sign(h)

        menu_connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        menu_channel = menu_connection.channel()
        menu_channel.exchange_declare(exchange='Promocoes', exchange_type='topic')

        menu_channel.basic_publish(
            exchange='Promocoes',
            routing_key=routing_key,
            body=body,
            properties=pika.BasicProperties(
            headers={
                "signature": signature,
                "vote": vote
            }
            )
        )

        menu_connection.close()

        return "Voto registrado com sucesso.", 200

def callback(ch, method, properties, body):
    print(f"Promoção publicada recebida. Verificando assinatura...")
    signature = properties.headers.get("signature")

    if signature is None:
        print("Evento sem assinatura.")
        return
    
    h = SHA256.new(body)

    valid_signature = False

    try:
        pkcs1_15.new(promocao_pub_key).verify(h, signature)
        valid_signature = True
        print("Assinatura válida.")
    except (ValueError, TypeError):
        print("Assinatura inválida.")
        
    if valid_signature:
        promocao = json.loads(body)
        promocoes[promocao['id']] = promocao

def consume():
    consume_connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    consume_channel = consume_connection.channel()
    consume_channel.exchange_declare(exchange='Promocoes', exchange_type='topic')

    result = consume_channel.queue_declare('fila_gateway', durable=True, exclusive=True)
    queue_name = result.method.queue

    consume_channel.queue_bind(exchange='Promocoes', queue=queue_name, routing_key='promocao.publicada')

    consume_channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)

    while not stop_event.is_set():
        consume_connection.process_data_events(time_limit=1)

    consume_connection.close()

if __name__ == '__main__':
    consume_thread = threading.Thread(
    target=consume,
    daemon=True
    )
    consume_thread.start()

    app.run(
        port=5000,
        debug=True,
        use_reloader=False
    )

    print("Encerrando o MS Gateway...")
    stop_event.set()