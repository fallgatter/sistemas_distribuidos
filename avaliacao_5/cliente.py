import json
import os
import re
import sys

import grpc

import raft_pb2
import raft_pb2_grpc


ASSIGNMENT_RE = re.compile(r'^\s*([A-Za-z_]\w*)\s*=\s*(.+?)\s*$')


def load_nodes_settings():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    settings_path = os.path.join(base_dir, 'nodes_settings.json')

    with open(settings_path, 'r', encoding='utf-8') as file:
        return json.load(file)


def resolve_target(nodes_settings, target):
    if target and ':' in target:
        return target

    if target in nodes_settings:
        node = nodes_settings[target]
        return f"{node['host']}:{node['port'] - 1000}"

    first_node = next(iter(nodes_settings.values()))
    return f"{first_node['host']}:{first_node['port'] - 1000}"


def build_stub(address):
    channel = grpc.insecure_channel(address)
    return raft_pb2_grpc.ClientStub(channel), channel


def parse_command(command):
    match = ASSIGNMENT_RE.match(command)
    if match:
        return 'put', match.group(1), match.group(2)

    parts = command.strip().split()
    if not parts:
        return None, None, None

    if len(parts) == 1:
        return 'get', parts[0], None

    return 'raw', command.strip(), None


def request_with_redirect(nodes_settings, address, kind, key, value=None):
    current_address = address

    while True:
        stub, channel = build_stub(current_address)
        try:
            if kind == 'put':
                response = stub.client_put(raft_pb2.ClientPutRequest(key=key, value=value), timeout=1.5)
            else:
                response = stub.client_get(raft_pb2.ClientGetRequest(key=key), timeout=1.5)
        except grpc.RpcError as error:
            channel.close()
            if error.code() in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED):
                current_address = resolve_target(nodes_settings, None)
                continue
            raise

        channel.close()

        status = response.status.strip()
        if status == 'Leader address follows' and response.value:
            current_address = response.value.strip()
            continue

        return response


def main():
    nodes_settings = load_nodes_settings()
    start_target = sys.argv[1] if len(sys.argv) > 1 else None
    current_address = resolve_target(nodes_settings, start_target)

    print('Digite comandos no formato "x = 10" para salvar ou apenas "x" para ler.')
    print('Digite "exit" ou "quit" para sair.')

    while True:
        try:
            command = input('raft> ').strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break

        if not command:
            continue

        if command.lower() in {'exit', 'quit'}:
            break

        kind, key, value = parse_command(command)
        if kind == 'put':
            response = request_with_redirect(nodes_settings, current_address, 'put', key, value)
            if response.status == 'Leader address follows' and response.value:
                current_address = response.value.strip()
            print(f'{response.status}: {response.value}')
        elif kind == 'get':
            response = request_with_redirect(nodes_settings, current_address, 'get', key)
            print(f'{response.status}: {response.value}')
        else:
            print('Comando inválido. Use "x = 10" ou apenas "x".')


if __name__ == '__main__':
    main()