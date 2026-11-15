import json
import os
import time
from pathlib import Path

import msgpack
import zmq


SERVER_NAME = os.getenv("SERVER_NAME", "servidor")
DATA_FILE = Path(os.getenv("DATA_FILE", "/dados/dados.json"))


def timestamp():
    return int(time.time() * 1000)


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
    result = {"ok": ok, "timestamp": timestamp(), "message": message}
    if data is not None:
        result["data"] = data
    return result


def process_request(request, state, publisher):
    if not isinstance(request, dict):
        return response(False, "A mensagem precisa ser um mapa MessagePack")

    if not isinstance(request.get("timestamp"), int):
        return response(False, "A mensagem precisa conter um timestamp inteiro")

    operation = request.get("op")

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


def main():
    state = load_state()
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    publisher = context.socket(zmq.PUB)
    socket.setsockopt(zmq.LINGER, 0)
    publisher.setsockopt(zmq.LINGER, 0)
    socket.bind("tcp://*:5555")
    publisher.connect(os.getenv("PROXY_PUB_URL", "tcp://proxy:5557"))
    print(f"[{SERVER_NAME}] aguardando mensagens na porta 5555", flush=True)

    try:
        while True:
            raw_message = socket.recv()
            request = None
            try:
                request = msgpack.unpackb(raw_message, raw=False)
                print_message("recebido", request)
                result = process_request(request, state, publisher)
            except (msgpack.UnpackException, ValueError, TypeError) as error:
                result = response(False, f"Mensagem invalida: {error}")

            if isinstance(request, dict):
                state["messages"].append({"request": request, "response": result})
                save_state(state)
            print_message("enviado", result)
            socket.send(msgpack.packb(result, use_bin_type=True))
    finally:
        socket.close()
        publisher.close()
        context.term()


if __name__ == "__main__":
    main()
