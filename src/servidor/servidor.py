import json
import os
import time
from pathlib import Path

import msgpack
import zmq


SERVER_NAME = os.getenv("SERVER_NAME", "servidor")
DATA_FILE = Path(os.getenv("DATA_FILE", "/dados/dados.json"))
REF_URL = os.getenv("REF_URL", "tcp://ref:5560")
PROXY_SUB_URL = os.getenv("PROXY_SUB_URL", "tcp://proxy:5558")
LOGICAL_CLOCK = 0
CLOCK_OFFSET = 0
SERVER_RANK = 0
COORDINATOR_NAME = None
ACTIVE_SERVERS = []
UNAVAILABLE_SERVERS = set()
HEARTBEAT_INTERVAL = 10
SYNC_INTERVAL = 15


def next_clock():
    global LOGICAL_CLOCK
    LOGICAL_CLOCK += 1
    return LOGICAL_CLOCK


def observe_clock(received):
    global LOGICAL_CLOCK
    LOGICAL_CLOCK = max(LOGICAL_CLOCK, received)


def timestamp():
    return int(time.time() * 1000) + CLOCK_OFFSET


def empty_state():
    return {"users": [], "logins": [], "channels": [], "messages": [], "publications": []}


def load_state():
    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    if not DATA_FILE.exists():
        return empty_state()

    with DATA_FILE.open("r", encoding="utf-8") as file:
        state = json.load(file)

    state.setdefault("users", [])
    state.setdefault("logins", [])
    state.setdefault("channels", [])
    state.setdefault("messages", [])
    state.setdefault("publications", [])
    return state


def save_state(state):
    temporary_file = DATA_FILE.with_suffix(".tmp")
    with temporary_file.open("w", encoding="utf-8") as file:
        json.dump(state, file, ensure_ascii=False, indent=2)
    temporary_file.replace(DATA_FILE)


def response(ok, message, data=None):
    result = {
        "ok": ok,
        "timestamp": timestamp(),
        "logical_clock": next_clock(),
        "message": message,
    }
    if data is not None:
        result["data"] = data
    return result


def process_request(request, state, publisher):
    if not isinstance(request, dict):
        return response(False, "A mensagem precisa ser um mapa MessagePack")

    if not isinstance(request.get("timestamp"), int):
        return response(False, "A mensagem precisa conter um timestamp inteiro")
    if not isinstance(request.get("logical_clock"), int):
        return response(False, "A mensagem precisa conter o contador lógico")

    operation = request.get("op")

    if operation == "election":
        return response(
            True,
            "OK",
            {"name": SERVER_NAME, "rank": SERVER_RANK},
        )

    if operation == "clock":
        global CLOCK_OFFSET
        if COORDINATOR_NAME != SERVER_NAME:
            return response(False, "Este servidor não é o coordenador")

        correct_time = (request["timestamp"] + timestamp()) // 2
        CLOCK_OFFSET = correct_time - int(time.time() * 1000)
        return response(True, "Hora correta", {"time": correct_time})

    if operation == "login":
        username = request.get("username")
        if not isinstance(username, str) or not username.strip():
            return response(False, "O nome do usuario não pode ficar vazio")

        username = username.strip()
        if username not in state["users"]:
            state["users"].append(username)

        login_timestamp = timestamp()
        state["logins"].append(
            {"username": username, "timestamp": login_timestamp}
        )
        return response(True, "Login realizado", {"username": username})

    username = request.get("username")
    if not isinstance(username, str) or username not in state["users"]:
        return response(False, "Faça login antes de usar esta operação")

    if operation == "list_channels":
        channels = sorted(channel["name"] for channel in state["channels"])
        return response(True, "Canais disponiveis", {"channels": channels})

    if operation == "create_channel":
        channel_name = request.get("channel")
        if not isinstance(channel_name, str):
            return response(False, "O nome do canal precisa ser texto")

        channel_name = channel_name.strip()
        if not channel_name or len(channel_name) > 64:
            return response(False, "O nome do canal deve ter de 1 a 64 caracteres")

        existing = next(
            (
                channel
                for channel in state["channels"]
                if channel["name"] == channel_name
            ),
            None,
        )
        if existing is not None:
            return response(
                True,
                "O canal ja existe",
                {"channel": channel_name, "created": False},
            )

        created_timestamp = timestamp()
        state["channels"].append(
            {
                "name": channel_name,
                "created_by": username,
                "timestamp": created_timestamp,
            }
        )
        return response(
            True,
            "Canal criado",
            {"channel": channel_name, "created": True},
        )

    if operation == "publish":
        channel_name = request.get("channel")
        message_text = request.get("message")
        if not isinstance(channel_name, str) or channel_name not in [
            channel["name"] for channel in state["channels"]
        ]:
            return response(False, "O canal não existe")
        if not isinstance(message_text, str) or not message_text:
            return response(False, "A mensagem não pode ficar vazia")

        publication = {
            "channel": channel_name,
            "username": username,
            "message": message_text,
            "timestamp": request["timestamp"],
            "sent_at": request["timestamp"],
            "received_at": timestamp(),
            "logical_clock": next_clock(),
        }
        topic = (channel_name + "\0").encode("utf-8")
        publisher.send_multipart(
            [topic, msgpack.packb(publication, use_bin_type=True)]
        )
        state["publications"].append(publication)
        return response(True, "Mensagem publicada", publication)

    return response(False, "Operação desconhecida")


def print_message(direction, message):
    readable_message = json.dumps(message, ensure_ascii=False)
    print(f"[{SERVER_NAME}] {direction}: {readable_message}", flush=True)


def ref_request(socket, operation, **fields):
    request = {
        "op": operation,
        "name": SERVER_NAME,
        "timestamp": timestamp(),
        "logical_clock": next_clock(),
    }
    request.update(fields)
    socket.send(msgpack.packb(request, use_bin_type=True))
    reply = msgpack.unpackb(socket.recv(), raw=False)
    observe_clock(reply["logical_clock"])
    return reply


def send_heartbeat(socket, rank):
    try:
        reply = ref_request(socket, "heartbeat", rank=rank)
        if reply.get("ok"):
            print(f"[{SERVER_NAME}] heartbeat; rank={rank}", flush=True)
        else:
            print(f"[{SERVER_NAME}] heartbeat recusado: {reply.get('message')}", flush=True)
    except (zmq.error.Again, OSError, KeyError) as error:
        print(f"[{SERVER_NAME}] falha no heartbeat: {error}", flush=True)


def announce_coordinator(publisher, name):
    publisher.send_multipart(
        [b"servers\0", msgpack.packb(name, use_bin_type=True)]
    )
    print(f"[{SERVER_NAME}] coordenador anunciado: {name}", flush=True)


def set_coordinator(name, publisher, announce=False):
    global COORDINATOR_NAME
    changed = name != COORDINATOR_NAME
    COORDINATOR_NAME = name
    if changed:
        print(f"[{SERVER_NAME}] coordenador: {COORDINATOR_NAME}", flush=True)
    if announce and name == SERVER_NAME:
        announce_coordinator(publisher, name)


def refresh_coordinator(ref_socket, publisher, announce=False):
    global ACTIVE_SERVERS
    try:
        reply = ref_request(ref_socket, "list")
        if reply.get("ok"):
            ACTIVE_SERVERS = reply.get("data", {}).get("servers", [])
    except (zmq.error.Again, OSError, KeyError) as error:
        print(f"[{SERVER_NAME}] falha ao consultar servidores: {error}", flush=True)

    servers = [
        server
        for server in ACTIVE_SERVERS
        if server.get("name") not in UNAVAILABLE_SERVERS
    ]
    if not any(server.get("name") == SERVER_NAME for server in servers):
        servers.append({"name": SERVER_NAME, "rank": SERVER_RANK})

    if servers:
        coordinator = max(servers, key=lambda server: server.get("rank", 0))
        set_coordinator(coordinator["name"], publisher, announce=announce)


def receive_coordinator(subscriber):
    global COORDINATOR_NAME
    parts = subscriber.recv_multipart()
    if len(parts) < 2 or parts[0] != b"servers\0":
        return

    try:
        name = msgpack.unpackb(parts[1], raw=False)
    except (msgpack.UnpackException, ValueError):
        return

    if isinstance(name, str) and name:
        COORDINATOR_NAME = name
        UNAVAILABLE_SERVERS.discard(name)
        print(f"[{SERVER_NAME}] coordenador recebido: {name}", flush=True)


def server_request(context, server_name, operation, **fields):
    peer_socket = context.socket(zmq.REQ)
    peer_socket.setsockopt(zmq.LINGER, 0)
    peer_socket.setsockopt(zmq.RCVTIMEO, 2000)
    peer_socket.setsockopt(zmq.SNDTIMEO, 2000)
    peer_socket.connect(f"tcp://{server_name}:5555")

    request = {
        "op": operation,
        "name": SERVER_NAME,
        "timestamp": timestamp(),
        "logical_clock": next_clock(),
    }
    request.update(fields)

    try:
        peer_socket.send(msgpack.packb(request, use_bin_type=True))
        reply = msgpack.unpackb(peer_socket.recv(), raw=False)
        observe_clock(reply["logical_clock"])
        return reply
    finally:
        peer_socket.close()


def elect_coordinator(context, ref_socket, publisher):
    global ACTIVE_SERVERS
    try:
        reply = ref_request(ref_socket, "list")
        if reply.get("ok"):
            ACTIVE_SERVERS = reply.get("data", {}).get("servers", [])
    except (zmq.error.Again, OSError, KeyError) as error:
        print(f"[{SERVER_NAME}] falha ao consultar servidores para eleição: {error}", flush=True)

    candidates = [{"name": SERVER_NAME, "rank": SERVER_RANK}]
    for server in ACTIVE_SERVERS:
        name = server.get("name")
        if not name or name == SERVER_NAME or name in UNAVAILABLE_SERVERS:
            continue

        try:
            reply = server_request(
                context,
                name,
                "election",
                candidate=SERVER_NAME,
                rank=SERVER_RANK,
            )
            if reply.get("ok"):
                candidates.append(
                    reply.get(
                        "data",
                        {"name": name, "rank": server.get("rank", 0)},
                    )
                )
                UNAVAILABLE_SERVERS.discard(name)
        except (zmq.error.ZMQError, OSError, KeyError, ValueError) as error:
            UNAVAILABLE_SERVERS.add(name)
            print(f"[{SERVER_NAME}] servidor {name} não respondeu à eleição: {error}", flush=True)

    coordinator = max(candidates, key=lambda server: server.get("rank", 0))
    set_coordinator(
        coordinator["name"],
        publisher,
        announce=(coordinator["name"] == SERVER_NAME),
    )


def synchronize_clock(context, ref_socket, publisher):
    if not COORDINATOR_NAME:
        refresh_coordinator(ref_socket, publisher)
    if not COORDINATOR_NAME or COORDINATOR_NAME == SERVER_NAME:
        return

    coordinator = COORDINATOR_NAME
    try:
        reply = server_request(context, coordinator, "clock")
        if not reply.get("ok"):
            raise OSError(reply.get("message", "Coordenador indisponível"))

        global CLOCK_OFFSET
        correct_time = reply["data"]["time"]
        CLOCK_OFFSET = correct_time - int(time.time() * 1000)
        UNAVAILABLE_SERVERS.discard(coordinator)
        print(f"[{SERVER_NAME}] relógio sincronizado com {coordinator}", flush=True)
    except (zmq.error.ZMQError, OSError, KeyError, ValueError) as error:
        UNAVAILABLE_SERVERS.add(coordinator)
        print(f"[{SERVER_NAME}] coordenador {coordinator} indisponível: {error}", flush=True)
        elect_coordinator(context, ref_socket, publisher)


def main():
    global SERVER_RANK
    state = load_state()
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    publisher = context.socket(zmq.PUB)
    subscriber = context.socket(zmq.SUB)
    ref_socket = context.socket(zmq.REQ)
    socket.setsockopt(zmq.LINGER, 0)
    publisher.setsockopt(zmq.LINGER, 0)
    subscriber.setsockopt(zmq.LINGER, 0)
    ref_socket.setsockopt(zmq.LINGER, 0)
    ref_socket.setsockopt(zmq.RCVTIMEO, 3000)
    subscriber.setsockopt(zmq.SUBSCRIBE, b"servers")
    ref_socket.connect(REF_URL)
    registration = ref_request(ref_socket, "rank")
    rank = registration["data"]["rank"]
    SERVER_RANK = rank
    socket.bind("tcp://*:5555")
    publisher.connect(os.getenv("PROXY_PUB_URL", "tcp://proxy:5557"))
    subscriber.connect(PROXY_SUB_URL)
    time.sleep(0.5)
    refresh_coordinator(ref_socket, publisher, announce=True)

    poller = zmq.Poller()
    poller.register(socket, zmq.POLLIN)
    poller.register(subscriber, zmq.POLLIN)
    print(f"[{SERVER_NAME}] rank {rank}, aguardando mensagens na porta 5555", flush=True)
    received_messages = 0

    try:
        while True:
            events = dict(poller.poll())
            if events.get(subscriber) == zmq.POLLIN:
                receive_coordinator(subscriber)
            if events.get(socket) != zmq.POLLIN:
                continue

            raw_message = socket.recv()
            request = None
            try:
                request = msgpack.unpackb(raw_message, raw=False)
                if isinstance(request, dict) and isinstance(request.get("logical_clock"), int):
                    observe_clock(request["logical_clock"])
                print_message("recebido", request)
                result = process_request(request, state, publisher)
            except (msgpack.UnpackException, ValueError, TypeError) as error:
                result = response(False, f"Mensagem invalida: {error}")

            internal_request = (
                isinstance(request, dict)
                and request.get("op") in ("clock", "election")
            )
            if isinstance(request, dict) and not internal_request:
                state["messages"].append({"request": request, "response": result})
                save_state(state)
            print_message("enviado", result)
            socket.send(msgpack.packb(result, use_bin_type=True))

            if not internal_request:
                received_messages += 1
            if not internal_request and received_messages % HEARTBEAT_INTERVAL == 0:
                send_heartbeat(ref_socket, rank)
                refresh_coordinator(ref_socket, publisher, announce=True)
            if not internal_request and received_messages % SYNC_INTERVAL == 0:
                synchronize_clock(context, ref_socket, publisher)
    finally:
        socket.close()
        publisher.close()
        subscriber.close()
        ref_socket.close()
        context.term()


if __name__ == "__main__":
    main()
