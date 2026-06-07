from functools import partial
import json
import pika
import threading
import os
from Crypto.Signature import pkcs1_15
from Crypto.Hash import SHA256
from Crypto.PublicKey import RSA
from flask import Flask, request
from queue import Queue

app = Flask(__name__)

pwd = b'senha'

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
clientes = {}
# espera um json do tipo:
# ({
#     "store_name": store_name,
#     "store_email": store_email,
#     "category": category.strip(),
#     "item_name": item_name,
#     "price": price,
#     "title": title,
#     "description": description
############     "signature": signature,
# })
@app.route('/cadastrar_promocao', methods=['POST'])
def cadastrar_promocao():
    promocao = request.get_json()

    if not promocao:
        return "Dados não presentes. Cadastro cancelado.", 400

    if not promocao["store_name"]:
        return "Nome da loja vazio. Cadastro cancelado.", 400
    
    if not promocao["store_email"]:
        return "Email da loja vazio. Cadastro cancelado.", 400

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

    if os.path.exists(f"./private_keys/stores/{promocao['store_name']}.pem"):
        with open(f"./private_keys/stores/{promocao['store_name']}.pem", "rb") as f:
            data = f.read()
            mykey = RSA.import_key(data, pwd)

    else: 
        mykey = RSA.generate(2048)
        with open(f"./private_keys/stores/{promocao['store_name']}.pem", "wb") as f:
            data = mykey.export_key(passphrase=pwd,
                                pkcs=8,
                                protection='PBKDF2WithHMAC-SHA512AndAES256-CBC',
                                prot_params={'iteration_count':131072})
            f.write(data)

        with open(f"./public_keys/stores/{promocao['store_name']}.pem", "wb") as f:
            public_key = mykey.publickey()
            data = public_key.export_key()
            f.write(data)

    body = json.dumps(promocao)
    h = SHA256.new(body.encode())
    signature = pkcs1_15.new(mykey).sign(h)
    
    menu_connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    menu_channel = menu_connection.channel()
    menu_channel.exchange_declare(exchange='Promocoes', exchange_type='topic')

    promocao_id_counter += 1

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

        return "Voto registrado com sucesso.", 201
    
def client_callback(client_id, ch, method, properties, body):
    promocao = json.loads(body)
    clientes[client_id]["queue"].put(promocao)
    
def client_consume(client_id, queue_name):
    clientes[client_id]["channel"].basic_consume(queue=queue_name, on_message_callback=partial(client_callback, client_id=client_id), auto_ack=True)

    while not stop_event.is_set():
        clientes[client_id]["channel"].process_data_events(time_limit=1)

# espera um json do tipo:
# ({
#     "client_id": client_id,
#     "category": category.strip()
# })
@app.route('/registrar_interesse', methods=['POST'])
def registrar_interesse():
    pedido = request.get_json()
    if not pedido:
        return "Dados não presentes. Registro de interesse cancelado.", 400
    if "client_id" not in pedido:
        return "ID do cliente vazio. Registro de interesse cancelado.", 400
    if "category" not in pedido:
        return "Categoria vazia. Registro de interesse cancelado.", 400
    
    with open('promocao_categorias.txt', 'r', encoding='utf-8') as f:
        categorys = f.readlines()
    try:
        category = categorys[int(pedido["category"]) - 1].strip()
    except (ValueError, IndexError):
        return "Categoria inválida. Registro de interesse cancelado.", 400
    
    queue_name = "fila_cliente" + pedido["client_id"]

    if pedido["client_id"] not in clientes:
        clientes[pedido["client_id"]] = {}
        clientes[pedido["client_id"]]["queue"] = Queue()
        clientes[pedido["client_id"]]["categorys"] = set()
        clientes[pedido["client_id"]]["connection"] = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        clientes[pedido["client_id"]]["channel"] = clientes[pedido["client_id"]]["connection"].channel()
        clientes[pedido["client_id"]]["channel"].exchange_declare(exchange='Promocoes', exchange_type='topic')
        result = clientes[pedido["client_id"]]["channel"].queue_declare(queue_name, durable=True, exclusive=True)
        threading.Thread(
            target=client_consume,
            args=(pedido["client_id"], queue_name)
        ).start()

    if category not in clientes[pedido["client_id"]]["categorys"]:
        if category == len(categorys):
            routing_key = "promocao.destaque"
        else:
            routing_key = f"promocao.categoria{pedido['category']}"
        clientes[pedido["client_id"]]["categorys"].add(category)
        clientes[pedido["client_id"]]["channel"].queue_bind(exchange='Promocoes', queue=queue_name, routing_key=routing_key)

    return "Interesse registrado com sucesso.", 201

@app.route('/remover_interesse', methods=['POST'])
def remover_interesse():
    pedido = request.get_json()
    if not pedido:
        return "Dados não presentes. Remoção de interesse cancelada.", 400
    if "client_id" not in pedido:
        return "ID do cliente vazio. Remoção de interesse cancelada.", 400
    if "category" not in pedido:
        return "Categoria vazia. Remoção de interesse cancelada.", 400

    category = pedido["category"]

    if category in clientes[pedido["client_id"]]["categorys"]:
        clientes[pedido["client_id"]]["categorys"].remove(category)
        queue_name = "fila_cliente" + pedido["client_id"]
        clientes[pedido["client_id"]]["channel"].queue_unbind(exchange='Promocoes', queue=queue_name, routing_key=f"promocao.categoria{pedido['category']}")
        return "Interesse removido com sucesso.", 200
    else:
        return "Categoria não encontrada entre os interesses do cliente. Remoção de interesse cancelada.", 400

# clien_id passado no url: /sse?client_id=123
@app.route('/sse', methods=['GET'])
def sse():
    def event_stream(client_id):
        while not stop_event.is_set():
            try:
                promocao = clientes[client_id]["queue"].get(timeout=1)
                yield f"data: {json.dumps(promocao)}\n\n"
            except:
                continue

    client_id = request.args.get('client_id')
    if not client_id:
        return "ID do cliente não fornecido. Conexão cancelada.", 400
    if client_id not in clientes:
        return "ID do cliente não registrado. Conexão cancelada.", 400

    return app.response_class(event_stream(client_id), mimetype='text/event-stream')

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
    target=consume
    )
    consume_thread.start()

    app.run(
        port=5000,
        debug=True,
        use_reloader=False
    )

    print("Encerrando o MS Gateway...")
    stop_event.set()