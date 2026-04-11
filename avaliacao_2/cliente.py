import json
import pika
import os

pid = str(os.getpid())

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
channel.exchange_declare(exchange='Promocoes', exchange_type='topic')

queue_name = "fila_cliente" + pid

result = channel.queue_declare(queue_name, exclusive=True)

def callback(ch, method, properties, body):
    
    promocao = json.loads(body)
    print(
        f"ID {promocao['id']} - {promocao['title']}\n"
        f"Categoria: {promocao['category']}\n"
        f"Item: {promocao['item_name']}\n"
        f"Valor: {promocao['price']}\n"
        f"Descrição: {promocao['description']}\n"
    )

print(f"Seja bem-vindo, cliente {pid}!")
print("\nDeseja receber notificações sobre promoções de quais categorias? Digite separado por vírgula (ex: 1, 3, 5):")
with open('promocao_categorias.txt', 'r', encoding='utf-8') as f:
    categorys_list = f.readlines()

for i, linha in enumerate(categorys_list):
    print(f"Categoria {i+1}: {linha.strip()}")

categorys_valid = 0
while not categorys_valid:
    try:
        categorys = input().split(',')
        categorys = [int(c.strip()) - 1 for c in categorys]
        categorys_valid = 1
        for c in categorys:
            if c < 0 or c >= len(categorys_list):
                categorys_valid = 0
                print(f"Categoria {c + 1} inválida. Digite novamente:")
    except EOFError:
        exit(1)
        
for c in categorys:
    print(f"Você optou por receber notificações de promoções da categoria {c + 1}.")
    routing_key = f"promocao.categoria{c}"
    channel.queue_bind(exchange='Promocoes', queue=queue_name, routing_key=routing_key)

channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)

channel.start_consuming()

connection.close()