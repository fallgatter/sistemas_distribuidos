import sys
import pika
import threading
from Crypto.Signature import pkcs1_15
from Crypto.Hash import SHA256
from Crypto.PublicKey import RSA

key = RSA.generate(2048)
private_key = key.export_key()
public_key = key.publickey().export_key()

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
                categorias = f.readlines()
            for i, linha in enumerate(categorias):
                print(f"Categoria {i+1}: {linha.strip()}")

            try:
                categoria = categorias[int(input()) - 1]
            except (ValueError, IndexError):
                print("Categoria inválida.")
                continue
            except EOFError:
                stop_event.set()
                break

            print("Digite a descrição da promoção:")
            try:
                body = input()
            except EOFError:
                stop_event.set()
                break

            routing_key = "promocao.recebida"
            h = SHA256.new(body.encode())
            signature = pkcs1_15.new(key).sign(h)

            menu_channel.basic_publish(
                exchange='Promocoes',
                routing_key=routing_key,
                body=body,
                properties=pika.BasicProperties(
                    headers={
                        "signature": signature,
                        "pub_key": public_key
                        #'categoria': categoria.strip().lower()
                    }
                )
            )
            print("Promoção cadastrada com sucesso!")

        elif interface == 2:
            print("Promoções publicadas:")
            for promocao in lista_promocoes:
                print(promocao)

    menu_connection.close()

def callback(ch, method, properties, body):
    print(f" [x] {method.routing_key}:{body}")

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