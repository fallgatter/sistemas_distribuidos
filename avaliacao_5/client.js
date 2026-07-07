const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');

const PROTO_PATH = path.join(__dirname, 'raft.proto');

const packageDef = protoLoader.loadSync(PROTO_PATH, {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true
});

raft = grpc.loadPackageDefinition(packageDef).raft;

nodes_settings = require('./nodes_settings.json');

leader_address = `${nodes_settings.node1.host}:${nodes_settings.node1.port - 1000}`;

function put(key, value) {
    const request = {key, value};
    tries = 0

    const sendPut = (address) => {
        const stub = new raft.Client(address, grpc.credentials.createInsecure());

        stub.client_put(request, (error, response) => {
            stub.close();

            if (error) {
                console.error(`Error in put request: ${error.message}`);
                if (error.code === grpc.status.UNAVAILABLE) {
                    console.log(`Node at ${address} is unavailable. Trying node ${tries + 1}...`);
                    if (tries < 3) {
                        tries++;
                        leader_address = `${nodes_settings[`node${tries}`].host}:${nodes_settings[`node${tries}`].port - 1000}`;
                        sendPut(leader_address);
                    }
                }
                rl.prompt();
                return;
            }

            console.log(`Put request status: ${response.status}, value: ${response.value}`);

            if (response.status === "Not the leader" && response.value) {
                console.log(`Redirecting to leader at ${response.value}`);
                leader_address = response.value.trim();
                sendPut(leader_address);
                return;
            }

            rl.prompt();
        });
    };

    sendPut(leader_address);
}

function get(node,key) {
    const address = `${nodes_settings[node].host}:${nodes_settings[node].port - 1000}`;
    const stub = new raft.Client(address, grpc.credentials.createInsecure());
    const request = {key};

    stub.client_get(request, (error, response) => { 
        stub.close();

        if (error) {
            console.error(`Error in get request: ${error.message}`);
            rl.prompt();
            return;
        }
        else{
            console.log(`[${node}] Get request status: ${response.status}, value: ${response.value}`);
        }

        rl.prompt();
    });
}

const readline = require('readline');

const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
    prompt: 'raft> '
});

console.log("Comandos:");
console.log("  PUT: chave = valor");
console.log("  GET: nodeX chave");
console.log("  Sair: exit");

rl.prompt();

rl.on('line', (line) => {

    line = line.trim();

    if (line === "exit") {
        rl.close();
        return;
    }

    if (line.includes('=')) { //put
        const partes = line.split('=');

        if (partes.length !== 2) {
            console.log("Formato inválido. Use: chave = valor");
        } else {
            const key = partes[0].trim();
            const value = partes[1].trim();

            put(key, value);
        }

    } else {
        // GET
        const partes = line.split(/\s+/);

        if (partes.length !== 2) {

            console.log("Use:");
            console.log("node1 chave");
            console.log("node2 chave");
            console.log("node3 chave");
            console.log("node4 chave");

            rl.prompt();
            return;
        }

        const node = partes[0];
        const key = partes[1];

        if (!nodes_settings[node]) {

            console.log("Nó inválido.");

            rl.prompt();
            return;
        }
        get(node, key);
    }

});

rl.on('close', () => {
    console.log("Cliente encerrado.");
    process.exit(0);
});

process.on('SIGINT', () => {
    rl.close();
});