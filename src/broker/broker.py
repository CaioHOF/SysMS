import zmq


context = zmq.Context()
clients = context.socket(zmq.ROUTER)
servers = context.socket(zmq.DEALER)
clients.setsockopt(zmq.LINGER, 0)
servers.setsockopt(zmq.LINGER, 0)
clients.bind("tcp://*:5555")
servers.bind("tcp://*:5556")

print("Broker Req-Rep aguardando clientes e servidores", flush=True)

try:
    zmq.proxy(clients, servers)
finally:
    clients.close()
    servers.close()
    context.term()
