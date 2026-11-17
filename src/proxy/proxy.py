import zmq


uContexto = zmq.Context()
uSocketXSub = uContexto.socket(zmq.XSUB)
uSocketXPub = uContexto.socket(zmq.XPUB)
uSocketXSub.bind("tcp://*:5557")
uSocketXPub.bind("tcp://*:5558")

print("Proxy Pub/Sub aguardando nas portas 5557 e 5558", flush=True)

try:
    zmq.proxy(uSocketXSub, uSocketXPub)
finally:
    uSocketXSub.close()
    uSocketXPub.close()
    uContexto.term()
