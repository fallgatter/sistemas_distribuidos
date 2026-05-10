import Pyro5.api

while True:

    command = input("Command: ")

    try:

        ns = Pyro5.api.locate_ns()

        leader_uri = ns.lookup("Leader")

        with Pyro5.api.Proxy(leader_uri) as leader:

            response = leader.client_request(command)

            print(response)

    except Exception as e:
        print("Error:", e)