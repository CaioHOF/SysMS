import zmq


context = zmq.Context()
xsub = context.socket(zmq.XSUB)
xpub = context.socket(zmq.XPUB)
xsub.bind("tcp://*:5557")
xpub.bind("tcp://*:5558")

print("Proxy Pub/Sub aguardando nas portas 5557 e 5558", flush=True)

try:
    zmq.proxy(xsub, xpub)
finally:
    xsub.close()
    xpub.close()
    context.term()
