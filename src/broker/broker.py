import zmq

uContexto = zmq.Context()
uSocketClientes = uContexto.socket(zmq.ROUTER)
uSocketServidores = uContexto.socket(zmq.DEALER)
# Faz o socket dos clientes não esperar mensagens pendentes quando fechar
uSocketClientes.setsockopt(zmq.LINGER, 0)
# A mesma coisa só que pro servidor
uSocketServidores.setsockopt(zmq.LINGER, 0)
uSocketClientes.bind("tcp://*:5555")
uSocketServidores.bind("tcp://*:5556")

print("Broker Req-Rep aguardando clientes e servidores...", flush=True)

#Faz a comunicação ficar ativa até ter alguma interrupção
try:
    zmq.proxy(uSocketClientes, uSocketServidores)

#Fecha todos os sokets e contexto
finally:
    uSocketClientes.close()
    uSocketServidores.close()
    uContexto.term()
