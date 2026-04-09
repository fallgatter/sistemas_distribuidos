import json
import pika
import threading
from Crypto.Signature import pkcs1_15
from Crypto.Hash import SHA256
from Crypto.PublicKey import RSA

key = RSA.generate(2048)

pwd = b'senha'
with open("./private_keys/msgateway_privatekey.pem", "wb") as f:
    data = key.export_key(passphrase=pwd,
                          pkcs=8,
                          protection='PBKDF2WithHMAC-SHA512AndAES256-CBC',
                          prot_params={'iteration_count':131072})
    f.write(data)

with open("./private_keys/msgateway_privatekey.pem", "rb") as f:
    data = f.read()
    mykey = RSA.import_key(data, pwd)

with open("./public_keys/msgateway_publickey.pem", "wb") as f:
    public_key = key.publickey()
    data = public_key.export_key()
    f.write(data)

stop_event = threading.Event()
lista_promocoes = []

def menu():
    menu_connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    menu_channel = menu_connection.channel()
    menu_channel.exchange_declare(exchange='Promocoes', exchange_type='topic')

    print("Bem vindo!\n")

    while not stop_event.is_set():
        menu_open = True

        while menu_open and not stop_event.is_set():
            print("\nSelecione a opção:\n"
                  "1 - Cadastrar nova promoção\n"
                  "2 - Listar promoções publicadas\n"
                  "3 - Votar em uma promoção existente\n"
                  "4 - Sair\n")
            try:
                interface = int(input())
            except (ValueError, EOFError):
                print("Entrada inválida.")
                continue

            if interface < 1 or interface > 4:
                print("Opção inválida, tente novamente.\n")
            else:
                menu_open = False

        if interface == 4:
            stop_event.set()

        if interface == 1:
            print("Escolha a categoria da promoção:")
            with open('promocao_categorias.txt', 'r', encoding='utf-8') as f:
                categorys = f.readlines()
            for i, linha in enumerate(categorys):
                print(f"Categoria {i+1}: {linha.strip()}")

            try:
                category = categorys[int(input()) - 1]
            except (ValueError, IndexError):
                print("Categoria inválida.")
                continue
            except EOFError:
                stop_event.set()
                break

            print("Digite o nome do item em promoção:")
            try:
                item_name = input()
            except EOFError:
                stop_event.set()
                break

            print("Digite o valor do item em promoção:")
            try:
                price = float(input())
                while price < 0:
                    print("Valor inválido. O preço deve ser um número positivo.")
                    price = float(input())
            except (ValueError, EOFError):
                print("Erro.")
                break
            
            print("Digite o título da promoção:")
            try:
                title = input()
            except EOFError:
                stop_event.set()
                break

            print("Digite a descrição da promoção:")
            try:
                description = input()
            except EOFError:
                stop_event.set()
                break

            routing_key = "promocao.recebida"
            
            body = json.dumps({
                "category": category.strip().lower(),
                "item_name": item_name,
                "price": price,
                "title": title,
                "description": description
            })

            h = SHA256.new(body.encode())
            signature = pkcs1_15.new(key).sign(h)

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
            print("Promoção cadastrada com sucesso!")

        elif interface == 2:
            if len(lista_promocoes) == 0:
                print("Nenhuma promoção disponível.")
            else:
                print("Promoções publicadas:")
                for i, promocao in enumerate(lista_promocoes):
                    print(f"{i+1}. {promocao}")

        elif interface == 3:
            if len(lista_promocoes) == 0:
                print("Nenhuma promoção disponível para votar.")
            else:
                print("Promoções publicadas:")
                for i, promocao in enumerate(lista_promocoes):
                    print(f"{i+1}. {promocao}")
                print("Digite o índice do item que deseja votar:")
                try:
                    item_index = int(input()) - 1
                    while 0 <= item_index < len(lista_promocoes):
                        print("Índice inválido. Digite um índice válido:")
                        item_index = int(input()) - 1
                    item_name = lista_promocoes[item_index]
                except (ValueError, EOFError):
                    print("Erro.")
                    break

                routing_key = "promocao.voto"
                body = json.dumps({
                    "item_name": item_name
                })

                menu_channel.basic_publish(
                    exchange='Promocoes',
                    routing_key=routing_key,
                    body=body
                )

                print("Voto registrado com sucesso!")

    menu_connection.close()

def callback(ch, method, properties, body):
    print(f"Promoção recebida. Verificando assinatura...")
    signature = properties.headers.get("signature")
    key = RSA.import_key(open('mspromocao_publickey.der').read())
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
        lista_promocoes.append(promocao)

def consume():
    consume_connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    consume_channel = consume_connection.channel()
    consume_channel.exchange_declare(exchange='Promocoes', exchange_type='topic')

    result = consume_channel.queue_declare('fila_gateway', exclusive=True)
    queue_name = result.method.queue

    consume_channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)

    while not stop_event.is_set():
        consume_connection.process_data_events(time_limit=1)

    consume_connection.close()

if __name__ == '__main__':
    consume_thread = threading.Thread(target=consume)
    consume_thread.start()

    menu_thread = threading.Thread(target=menu)
    menu_thread.start()

    menu_thread.join()

    consume_thread.join()