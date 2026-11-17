import zmq


uContexto = zmq.Context()
uSocketClientes = uContexto.socket(zmq.ROUTER)
uSocketServidores = uContexto.socket(zmq.DEALER)
uSocketClientes.setsockopt(zmq.LINGER, 0)
uSocketServidores.setsockopt(zmq.LINGER, 0)
uSocketClientes.bind("tcp://*:5555")
uSocketServidores.bind("tcp://*:5556")

print("...Broker Req-Rep aguardando clientes e servidores...", flush=True)

try:
    zmq.proxy(uSocketClientes, uSocketServidores)
finally:
    uSocketClientes.close()
    uSocketServidores.close()
    uContexto.term()
