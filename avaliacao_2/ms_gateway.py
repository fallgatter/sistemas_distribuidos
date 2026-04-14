import json
import pika
import threading
import os
from Crypto.Signature import pkcs1_15
from Crypto.Hash import SHA256
from Crypto.PublicKey import RSA

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
lista_promocoes = {}

def menu():
    id_counter = 0

    print("Bem vindo!\n")

    while not stop_event.is_set():
        menu_open = True
        interface = 0

        while menu_open and not stop_event.is_set():
            print("\nSelecione a opção:\n"
                  "1 - Cadastrar nova promoção\n"
                  "2 - Listar promoções publicadas\n"
                  "3 - Votar em uma promoção existente\n"
                  "4 - Sair\n")
            try:
                interface = int(input())
                if interface < 1 or interface > 4:
                    print("Opção inválida, tente novamente.\n")
            except ValueError:
                print("Opção inválida, tente novamente.\n")
                continue
            except (EOFError, KeyboardInterrupt):
                stop_event.set()
                break

            menu_open = False

        if interface == 4:
            stop_event.set()

        if interface == 1:
            print("\nEscolha a categoria da promoção:")
            with open('promocao_categorias.txt', 'r', encoding='utf-8') as f:
                categorys = f.readlines()
            for i, linha in enumerate(categorys):
                print(f"Categoria {i+1}: {linha.strip()}")

            try:
                category = categorys[int(input()) - 1]
            except (ValueError, IndexError):
                print("Categoria inválida.")
                continue
            except (EOFError, KeyboardInterrupt):
                stop_event.set()
                break

            print("\nDigite o nome do item em promoção:")
            try:
                item_name = input()
            except (EOFError, KeyboardInterrupt):
                stop_event.set()
                break

            print("\nDigite o valor do item em promoção:")
            try:
                price = float(input())
                while price < 0:
                    print("Valor inválido. O preço deve ser um número positivo.")
                    price = float(input())
            except ValueError:
                print("Valor inválido.")
                continue
            except (EOFError, KeyboardInterrupt):
                stop_event.set()
                break
            
            print("\nDigite o título da promoção:")
            try:
                title = input()
            except (EOFError, KeyboardInterrupt):
                stop_event.set()
                break

            print("\nDigite a descrição da promoção:")
            try:
                description = input()
            except (EOFError, KeyboardInterrupt):
                stop_event.set()
                break

            routing_key = "promocao.recebida"
            
            body = json.dumps({
                "id": id_counter,
                "category": category.strip(),
                "item_name": item_name,
                "price": price,
                "title": title,
                "description": description
            })

            menu_connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
            menu_channel = menu_connection.channel()
            menu_channel.exchange_declare(exchange='Promocoes', exchange_type='topic')

            id_counter += 1

            h = SHA256.new(body.encode())
            signature = pkcs1_15.new(mykey).sign(h)

            menu_channel.basic_publish(
                exchange='Promocoes',
                routing_key=routing_key,
                body=body,
                properties=pika.BasicProperties(
                    headers={
                        "signature": signature
                    }
                )
            )

            menu_connection.close()
            print("Promoção cadastrada com sucesso!")

        elif interface == 2:
            if len(lista_promocoes) == 0:
                print("Nenhuma promoção disponível.")
            else:
                print("\nPromoções publicadas:")
                for promocao in lista_promocoes.values():
                    print(
                        f"ID {promocao['id']} - {promocao['title']}\n"
                        f"Categoria: {promocao['category']}\n"
                        f"Item: {promocao['item_name']}\n"
                        f"Valor: {promocao['price']}\n"
                        f"Descrição: {promocao['description']}\n"
                    )

        elif interface == 3:
            if len(lista_promocoes) == 0:
                print("Nenhuma promoção disponível para votar.")
            else:   
                print("\nPromoções publicadas:")
                for promocao in lista_promocoes.values():
                    print(
                        f"ID {promocao['id']} - {promocao['title']}\n"
                        f"Categoria: {promocao['category']}\n"
                        f"Item: {promocao['item_name']}\n"
                        f"Valor: {promocao['price']}\n"
                        f"Descrição: {promocao['description']}\n"
                    )

                print("\nDigite o ID do item que deseja votar:")
                try:
                    item_id = int(input())
                    while item_id not in lista_promocoes:
                        print("ID inválido. Digite um ID válido:")
                        item_id = int(input())
                except (EOFError, KeyboardInterrupt):
                    stop_event.set()
                    break

                print("\nDigite 1 para votar positivamente ou 0 para votar negativamente:")
                try:
                    vote = int(input())
                    while vote not in [0, 1]:
                        print("Opção inválida. Digite 1 para votar positivamente ou 0 para votar negativamente:")
                        vote = int(input())
                    if vote == 1:
                        vote = "upvote"
                    else:
                        vote = "downvote"
                except (EOFError, KeyboardInterrupt):
                    stop_event.set()
                    break

                routing_key = "promocao.voto"
                body = json.dumps(lista_promocoes[item_id])

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

                print("\nVoto registrado com sucesso!")

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
        lista_promocoes[promocao['id']] = promocao

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
    consume_thread = threading.Thread(target=consume)
    consume_thread.start()

    menu_thread = threading.Thread(target=menu)
    menu_thread.start()
    
    try:
        menu_thread.join()
        consume_thread.join()
    except KeyboardInterrupt:
        exit(1)