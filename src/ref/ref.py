import time

import msgpack
import zmq


SERVERS = {}
LAST_HEARTBEAT = {}
NEXT_RANK = 1
LOGICAL_CLOCK = 0
HEARTBEAT_TIMEOUT = 30


def timestamp():
    return int(time.time() * 1000)


def response(ok, message, data=None):
    #coloquei global pra facilitar
    global LOGICAL_CLOCK
    LOGICAL_CLOCK += 1
    result = {
        "ok": ok,
        "timestamp": timestamp(),
        "logical_clock": LOGICAL_CLOCK,
        "message": message,
    }
    if data is not None:
        result["data"] = data
    return result


def active_servers():
    now = time.monotonic()
    for name, last_seen in list(LAST_HEARTBEAT.items()):
        if now - last_seen > HEARTBEAT_TIMEOUT:
            del LAST_HEARTBEAT[name]
    return sorted(
        [{"name": name, "rank": rank} for name, rank in SERVERS.items()
         if name in LAST_HEARTBEAT],
        key=lambda server: server["rank"],
    )


def process_request(request):
    global LOGICAL_CLOCK, NEXT_RANK
    if not isinstance(request, dict):
        return response(False, "A mensagem precisa ser um mapa MessagePack")
    if not isinstance(request.get("timestamp"), int):
        return response(False, "A mensagem precisa conter timestamp")
    if not isinstance(request.get("logical_clock"), int):
        return response(False, "A mensagem precisa conter contador lógico")

    LOGICAL_CLOCK = max(LOGICAL_CLOCK, request["logical_clock"])
    operation = request.get("op")
    name = request.get("name")

    if operation == "list":
        return response(True, "Servidores disponíveis", {"servers": active_servers()})

    if not isinstance(name, str) or not name:
        return response(False, "Nome do servidor inválido")

    if operation == "rank":
        if name not in SERVERS:
            SERVERS[name] = NEXT_RANK
            NEXT_RANK += 1
        LAST_HEARTBEAT[name] = time.monotonic()
        return response(True, "Rank do servidor", {"rank": SERVERS[name]})

    if operation == "heartbeat":
        if name not in SERVERS or request.get("rank") != SERVERS[name]:
            return response(False, "Servidor não registrado")
        LAST_HEARTBEAT[name] = time.monotonic()
        return response(True, "Heartbeat recebido", {
            "rank": SERVERS[name],
            "time": timestamp(),
        })

    return response(False, "Operação desconhecida")


def main():
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.setsockopt(zmq.LINGER, 0)
    socket.bind("tcp://*:5560")
    print("Serviço ref aguardando na porta 5560", flush=True)

    try:
        while True:
            raw_message = socket.recv()
            try:
                request = msgpack.unpackb(raw_message, raw=False)
                print("ref recebeu:", request, flush=True)
                result = process_request(request)
            except (msgpack.UnpackException, ValueError, TypeError) as error:
                result = response(False, f"Mensagem inválida: {error}")
            print("ref enviou:", result, flush=True)
            socket.send(msgpack.packb(result, use_bin_type=True))
    finally:
        socket.close()
        context.term()


if __name__ == "__main__":
    main()
