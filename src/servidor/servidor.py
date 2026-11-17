import json
import os
import time
import uuid
from pathlib import Path

import msgpack
import zmq


uNomeServidor = os.getenv("SERVER_NAME", "servidor")
uArquivoDados = Path(os.getenv("DATA_FILE", "/dados/dados.json"))
uUrlReferencia = os.getenv("REF_URL", "tcp://ref:5560")
uUrlBroker = os.getenv("BROKER_URL", "tcp://broker:5556")
uUrlProxyAssinante = os.getenv("PROXY_SUB_URL", "tcp://proxy:5558")
uRelogioLogico = 0
uDesvioRelogio = 0
uRankServidor = 0
uNomeCoordenador = None
vtServidoresAtivos = []
uServidoresConhecidos = {}
uServidoresIndisponiveis = set()
uIntervaloHeartbeat = 10
uIntervaloSincronizacao = 15


def iProximoRelogio():
    global uRelogioLogico
    uRelogioLogico += 1
    return uRelogioLogico


def vAtualizarRelogioLogico(uRelogioRecebido):
    global uRelogioLogico
    uRelogioLogico = max(uRelogioLogico, uRelogioRecebido)


def iObterTimestamp():
    return int(time.time() * 1000) + uDesvioRelogio


def mCriarEstadoVazio():
    return {"users": [], "logins": [], "channels": [], "messages": [], "publications": []}


def mCarregarEstado():
    uArquivoDados.parent.mkdir(parents=True, exist_ok=True)
    if not uArquivoDados.exists():
        return mCriarEstadoVazio()

    with uArquivoDados.open("r", encoding="utf-8") as uArquivo:
        uEstado = json.load(uArquivo)

    uEstado.setdefault("users", [])
    uEstado.setdefault("logins", [])
    uEstado.setdefault("channels", [])
    uEstado.setdefault("messages", [])
    uEstado.setdefault("publications", [])
    return uEstado


def vSalvarEstado(uEstado):
    uArquivoTemporario = uArquivoDados.with_suffix(".tmp")
    with uArquivoTemporario.open("w", encoding="utf-8") as uArquivo:
        json.dump(uEstado, uArquivo, ensure_ascii=False, indent=2)
    uArquivoTemporario.replace(uArquivoDados)


def mCriarResposta(uSucesso, uMensagemResposta, uDados=None):
    uResultado = {
        "ok": uSucesso,
        "timestamp": iObterTimestamp(),
        "logical_clock": iProximoRelogio(),
        "message": uMensagemResposta,
    }
    if uDados is not None:
        uResultado["data"] = uDados
    return uResultado


def mProcessarPedido(uRequisicao, uEstado, uPublicador):
    if not isinstance(uRequisicao, dict):
        return mCriarResposta(False, "A mensagem precisa ser um mapa MessagePack")

    if not isinstance(uRequisicao.get("timestamp"), int):
        return mCriarResposta(False, "A mensagem precisa conter um timestamp inteiro")
    if not isinstance(uRequisicao.get("logical_clock"), int):
        return mCriarResposta(False, "A mensagem precisa conter o contador lógico")

    uOperacao = uRequisicao.get("op")

    if uOperacao == "election":
        return mCriarResposta(
            True,
            "OK",
            {"name": uNomeServidor, "rank": uRankServidor},
        )

    if uOperacao == "clock":
        global uDesvioRelogio
        if uNomeCoordenador != uNomeServidor:
            return mCriarResposta(False, "Este servidor não é o coordenador")

        uHoraCorreta = (uRequisicao["timestamp"] + iObterTimestamp()) // 2
        uDesvioRelogio = uHoraCorreta - int(time.time() * 1000)
        return mCriarResposta(True, "Hora correta", {"time": uHoraCorreta})

    if uOperacao == "login":
        uNomeUsuario = uRequisicao.get("username")
        if not isinstance(uNomeUsuario, str) or not uNomeUsuario.strip():
            return mCriarResposta(False, "O nome do usuario não pode ficar vazio")

        uNomeUsuario = uNomeUsuario.strip()
        if uNomeUsuario not in uEstado["users"]:
            uEstado["users"].append(uNomeUsuario)

        uHorarioLogin = iObterTimestamp()
        uEstado["logins"].append(
            {"username": uNomeUsuario, "timestamp": uHorarioLogin}
        )
        return mCriarResposta(True, "Login realizado", {"username": uNomeUsuario})

    uNomeUsuario = uRequisicao.get("username")
    if not isinstance(uNomeUsuario, str) or uNomeUsuario not in uEstado["users"]:
        return mCriarResposta(False, "Faça login antes de usar esta operação")

    if uOperacao == "list_channels":
        vtCanais = sorted(uCanal["name"] for uCanal in uEstado["channels"])
        return mCriarResposta(True, "Canais disponiveis", {"channels": vtCanais})

    if uOperacao == "list_messages":
        uNomeCanal = uRequisicao.get("channel")
        vtMensagens = uEstado["publications"]
        if isinstance(uNomeCanal, str):
            vtMensagens = [
                uMensagem for uMensagem in vtMensagens
                if uMensagem["channel"] == uNomeCanal
            ]
        vtMensagens = sorted(
            vtMensagens,
            key=lambda uMensagem: (
                uMensagem.get("sent_at", uMensagem.get("timestamp", 0)),
                uMensagem.get("event_id", ""),
                uMensagem.get("username", ""),
                uMensagem.get("message", ""),
            ),
        )
        uDeslocamento = uRequisicao.get("offset", 0)
        uLimite = uRequisicao.get("limit", 100)
        if not isinstance(uDeslocamento, int) or uDeslocamento < 0:
            return mCriarResposta(False, "O offset do historico precisa ser positivo")
        if not isinstance(uLimite, int) or uLimite < 1:
            return mCriarResposta(False, "O limite do historico precisa ser positivo")
        uLimite = min(uLimite, 100)
        return mCriarResposta(
            True,
            "Historico de mensagens",
            {
                "messages": vtMensagens[uDeslocamento:uDeslocamento + uLimite],
                "offset": uDeslocamento,
                "total": len(vtMensagens),
                "more": uDeslocamento + uLimite < len(vtMensagens),
            },
        )

    if uOperacao == "create_channel":
        uNomeCanal = uRequisicao.get("channel")
        if not isinstance(uNomeCanal, str):
            return mCriarResposta(False, "O nome do canal precisa ser texto")

        uNomeCanal = uNomeCanal.strip()
        if not uNomeCanal or len(uNomeCanal) > 64:
            return mCriarResposta(False, "O nome do canal deve ter de 1 a 64 caracteres")

        uExistente = next(
            (
                uCanal
                for uCanal in uEstado["channels"]
                if uCanal["name"] == uNomeCanal
            ),
            None,
        )
        if uExistente is not None:
            return mCriarResposta(
                True,
                "O canal ja existe",
                {"channel": uNomeCanal, "created": False},
            )

        uHorarioCriacao = iObterTimestamp()
        uEstado["channels"].append(
            {
                "name": uNomeCanal,
                "created_by": uNomeUsuario,
                "timestamp": uHorarioCriacao,
            }
        )
        return mCriarResposta(
            True,
            "Canal criado",
            {"channel": uNomeCanal, "created": True},
        )

    if uOperacao == "publish":
        uNomeCanal = uRequisicao.get("channel")
        uTextoMensagem = uRequisicao.get("message")
        if not isinstance(uNomeCanal, str) or uNomeCanal not in [
            uCanal["name"] for uCanal in uEstado["channels"]
        ]:
            return mCriarResposta(False, "O canal não existe")
        if not isinstance(uTextoMensagem, str) or not uTextoMensagem:
            return mCriarResposta(False, "A mensagem não pode ficar vazia")

        uPublicacao = {
            "channel": uNomeCanal,
            "username": uNomeUsuario,
            "message": uTextoMensagem,
            "timestamp": uRequisicao["timestamp"],
            "sent_at": uRequisicao["timestamp"],
            "received_at": iObterTimestamp(),
            "logical_clock": iProximoRelogio(),
        }
        vtTopico = (uNomeCanal + "\0").encode("utf-8")
        uPublicador.send_multipart(
            [vtTopico, msgpack.packb(uPublicacao, use_bin_type=True)]
        )
        uEstado["publications"].append(uPublicacao)
        return mCriarResposta(True, "Mensagem publicada", uPublicacao)

    return mCriarResposta(False, "Operação desconhecida")


def vExibirMensagem(uDirecao, uMensagem):
    uMensagemFormatada = json.dumps(uMensagem, ensure_ascii=False)
    print(f"[{uNomeServidor}] {uDirecao}: {uMensagemFormatada}", flush=True)


def mSolicitarReferencia(uSocket, uOperacao, **uCampos):
    uRequisicao = {
        "op": uOperacao,
        "name": uNomeServidor,
        "timestamp": iObterTimestamp(),
        "logical_clock": iProximoRelogio(),
    }
    uRequisicao.update(uCampos)
    uSocket.send(msgpack.packb(uRequisicao, use_bin_type=True))
    uResposta = msgpack.unpackb(uSocket.recv(), raw=False)
    vAtualizarRelogioLogico(uResposta["logical_clock"])
    return uResposta


def vEnviarHeartbeat(uSocket, uRank):
    try:
        uResposta = mSolicitarReferencia(uSocket, "heartbeat", rank=uRank)
        if uResposta.get("ok"):
            print(f"[{uNomeServidor}] heartbeat; rank={uRank}", flush=True)
        else:
            print(f"[{uNomeServidor}] heartbeat recusado: {uResposta.get('message')}", flush=True)
    except (zmq.error.Again, OSError, KeyError) as uErro:
        print(f"[{uNomeServidor}] falha no heartbeat: {uErro}", flush=True)


def vAnunciarCoordenador(uPublicador, uNome):
    uPublicador.send_multipart(
        [
            b"servers\0",
            msgpack.packb(
                {
                    "coordinator": uNome,
                    "timestamp": iObterTimestamp(),
                    "logical_clock": iProximoRelogio(),
                },
                use_bin_type=True,
            ),
        ]
    )
    print(f"[{uNomeServidor}] coordenador anunciado: {uNome}", flush=True)


def vDefinirCoordenador(uNome, uPublicador, uAnunciar=False):
    global uNomeCoordenador
    uAlterado = uNome != uNomeCoordenador
    uNomeCoordenador = uNome
    if uAlterado:
        print(f"[{uNomeServidor}] coordenador: {uNomeCoordenador}", flush=True)
    if uAnunciar:
        vAnunciarCoordenador(uPublicador, uNome)


def vAtualizarCoordenador(uSocketReferencia, uPublicador, uAnunciar=False):
    global vtServidoresAtivos
    try:
        uResposta = mSolicitarReferencia(uSocketReferencia, "list")
        if uResposta.get("ok"):
            vtServidoresAtivos = uResposta.get("data", {}).get("servers", [])
            for uServidor in vtServidoresAtivos:
                uServidoresConhecidos[uServidor["name"]] = uServidor["rank"]
    except (zmq.error.Again, OSError, KeyError) as uErro:
        print(f"[{uNomeServidor}] falha ao consultar servidores: {uErro}", flush=True)

    vtNomesAtivos = {uServidor.get("name") for uServidor in vtServidoresAtivos}
    if uNomeCoordenador and uNomeCoordenador not in vtNomesAtivos:
        return

    vtServidores = [
        uServidor
        for uServidor in vtServidoresAtivos
        if uServidor.get("name") not in uServidoresIndisponiveis
    ]
    if not any(uServidor.get("name") == uNomeServidor for uServidor in vtServidores):
        vtServidores.append({"name": uNomeServidor, "rank": uRankServidor})

    if vtServidores:
        uCoordenador = max(vtServidores, key=lambda uServidor: uServidor.get("rank", 0))
        vDefinirCoordenador(uCoordenador["name"], uPublicador, uAnunciar=uAnunciar)


def sChaveItemReplica(uItem):
    if isinstance(uItem, dict) and uItem.get("event_id"):
        return uItem["event_id"]
    return json.dumps(uItem, ensure_ascii=False, sort_keys=True)


def bMesclarItens(vtDestino, vtRecebidos, uFuncaoChave):
    uChaves = {uFuncaoChave(uItem) for uItem in vtDestino}
    uAlterado = False
    for uItem in vtRecebidos:
        uChave = uFuncaoChave(uItem)
        if uChave not in uChaves:
            vtDestino.append(uItem)
            uChaves.add(uChave)
            uAlterado = True
    return uAlterado


def bMesclarCanais(vtDestino, vtRecebidos):
    uPosicoes = {uCanal["name"]: uIndice for uIndice, uCanal in enumerate(vtDestino)}
    uAlterado = False
    for uCanal in vtRecebidos:
        uNome = uCanal["name"]
        if uNome not in uPosicoes:
            uPosicoes[uNome] = len(vtDestino)
            vtDestino.append(uCanal)
            uAlterado = True
            continue

        uIndice = uPosicoes[uNome]
        uAtual = vtDestino[uIndice]
        uChaveAtual = (uAtual.get("timestamp", 0), uAtual.get("created_by", ""))
        uChaveRecebida = (uCanal.get("timestamp", 0), uCanal.get("created_by", ""))
        if uChaveRecebida < uChaveAtual:
            vtDestino[uIndice] = uCanal
            uAlterado = True
    return uAlterado


def bMesclarEstadoReplica(uEstado, uReplica):
    uAlterado = False
    uAlterado |= bMesclarItens(uEstado["users"], uReplica.get("users", []), lambda uItem: uItem)
    uAlterado |= bMesclarItens(uEstado["logins"], uReplica.get("logins", []), sChaveItemReplica)
    uAlterado |= bMesclarCanais(uEstado["channels"], uReplica.get("channels", []))
    uAlterado |= bMesclarItens(uEstado["messages"], uReplica.get("messages", []), sChaveItemReplica)
    uAlterado |= bMesclarItens(uEstado["publications"], uReplica.get("publications", []), sChaveItemReplica)
    return uAlterado


def vPublicarEventoReplica(uPublicador, uEvento):
    uEvento["timestamp"] = iObterTimestamp()
    uEvento["logical_clock"] = iProximoRelogio()
    uPublicador.send_multipart(
        [b"replica_event\0", msgpack.packb(uEvento, use_bin_type=True)]
    )


def vSolicitarCopiaEstado(uPublicador):
    uPublicador.send_multipart(
        [
            b"replica_request\0",
            msgpack.packb(
                {
                    "source": uNomeServidor,
                    "timestamp": iObterTimestamp(),
                    "logical_clock": iProximoRelogio(),
                },
                use_bin_type=True,
            ),
        ]
    )


def vReceberMensagemPubSub(uAssinante, uPublicador, uEstado, uFontesCopia=None):
    global uNomeCoordenador
    vtPartes = uAssinante.recv_multipart()
    if len(vtPartes) < 2:
        return

    try:
        uMensagem = msgpack.unpackb(vtPartes[1], raw=False)
    except (msgpack.UnpackException, ValueError):
        return

    vtTopico = vtPartes[0]
    if vtTopico == b"servers\0":
        uNome = uMensagem.get("coordinator")
        if isinstance(uMensagem.get("logical_clock"), int):
            vAtualizarRelogioLogico(uMensagem["logical_clock"])
        if isinstance(uNome, str) and uNome:
            uNomeCoordenador = uNome
            uServidoresIndisponiveis.discard(uNome)
            print(f"[{uNomeServidor}] coordenador recebido: {uNome}", flush=True)
        return

    if not isinstance(uMensagem, dict) or uMensagem.get("source") == uNomeServidor:
        return
    if isinstance(uMensagem.get("logical_clock"), int):
        vAtualizarRelogioLogico(uMensagem["logical_clock"])

    if vtTopico == b"replica_request\0":
        uCopiaEstado = {
            "source": uNomeServidor,
            "state": uEstado,
            "timestamp": iObterTimestamp(),
            "logical_clock": iProximoRelogio(),
        }
        uPublicador.send_multipart(
            [b"replica_state\0", msgpack.packb(uCopiaEstado, use_bin_type=True)]
        )
    elif vtTopico == b"replica_state\0":
        if uFontesCopia is not None and isinstance(uMensagem.get("source"), str):
            uFontesCopia.add(uMensagem["source"])
        if bMesclarEstadoReplica(uEstado, uMensagem.get("state", {})):
            vSalvarEstado(uEstado)
    elif vtTopico == b"replica_event\0":
        uReplica = dict(uMensagem.get("changes", {}))
        if uMensagem.get("message"):
            uReplica["messages"] = [uMensagem["message"]]
        if bMesclarEstadoReplica(uEstado, uReplica):
            vSalvarEstado(uEstado)


def mSolicitarServidor(uContexto, uNomeServidorLocal, uOperacao, **uCampos):
    uSocketPar = uContexto.socket(zmq.REQ)
    uSocketPar.setsockopt(zmq.LINGER, 0)
    uSocketPar.setsockopt(zmq.RCVTIMEO, 2000)
    uSocketPar.setsockopt(zmq.SNDTIMEO, 2000)
    uSocketPar.connect(f"tcp://{uNomeServidorLocal}:5561")

    uRequisicao = {
        "op": uOperacao,
        "name": uNomeServidor,
        "timestamp": iObterTimestamp(),
        "logical_clock": iProximoRelogio(),
    }
    uRequisicao.update(uCampos)

    try:
        uSocketPar.send(msgpack.packb(uRequisicao, use_bin_type=True))
        uResposta = msgpack.unpackb(uSocketPar.recv(), raw=False)
        vAtualizarRelogioLogico(uResposta["logical_clock"])
        return uResposta
    finally:
        uSocketPar.close()


def vElegerCoordenador(uContexto, uSocketReferencia, uPublicador):
    global vtServidoresAtivos
    try:
        uResposta = mSolicitarReferencia(uSocketReferencia, "list")
        if uResposta.get("ok"):
            vtServidoresAtivos = uResposta.get("data", {}).get("servers", [])
            for uServidor in vtServidoresAtivos:
                uServidoresConhecidos[uServidor["name"]] = uServidor["rank"]
    except (zmq.error.Again, OSError, KeyError) as uErro:
        print(f"[{uNomeServidor}] falha ao consultar servidores para eleição: {uErro}", flush=True)

    vtCandidatos = [{"name": uNomeServidor, "rank": uRankServidor}]
    for uNome, uRank in uServidoresConhecidos.items():
        if not uNome or uNome == uNomeServidor:
            continue

        try:
            uResposta = mSolicitarServidor(
                uContexto,
                uNome,
                "election",
                candidate=uNomeServidor,
                rank=uRankServidor,
            )
            if uResposta.get("ok"):
                vtCandidatos.append(
                    uResposta.get(
                        "data",
                        {"name": uNome, "rank": uRank},
                    )
                )
                uServidoresIndisponiveis.discard(uNome)
        except (zmq.error.ZMQError, OSError, KeyError, ValueError) as uErro:
            uServidoresIndisponiveis.add(uNome)
            print(f"[{uNomeServidor}] servidor {uNome} não respondeu à eleição: {uErro}", flush=True)

    uCoordenador = max(vtCandidatos, key=lambda uServidor: uServidor.get("rank", 0))
    vDefinirCoordenador(
        uCoordenador["name"],
        uPublicador,
        uAnunciar=True,
    )


def vSincronizarRelogio(uContexto, uSocketReferencia, uPublicador):
    if not uNomeCoordenador:
        vAtualizarCoordenador(uSocketReferencia, uPublicador)
    if not uNomeCoordenador or uNomeCoordenador == uNomeServidor:
        return

    uNomeCoordenadorLocal = uNomeCoordenador
    try:
        uResposta = mSolicitarServidor(uContexto, uNomeCoordenadorLocal, "clock")
        if not uResposta.get("ok"):
            raise OSError(uResposta.get("message", "Coordenador indisponível"))

        global uDesvioRelogio
        uHoraCorreta = uResposta["data"]["time"]
        uDesvioRelogio = uHoraCorreta - int(time.time() * 1000)
        uServidoresIndisponiveis.discard(uNomeCoordenadorLocal)
        print(f"[{uNomeServidor}] relógio sincronizado com {uNomeCoordenadorLocal}", flush=True)
    except (zmq.error.ZMQError, OSError, KeyError, ValueError) as uErro:
        uServidoresIndisponiveis.add(uNomeCoordenadorLocal)
        print(f"[{uNomeServidor}] coordenador {uNomeCoordenadorLocal} indisponível: {uErro}", flush=True)
        vElegerCoordenador(uContexto, uSocketReferencia, uPublicador)


def vExecutarServidor():
    global uRankServidor
    uEstado = mCarregarEstado()
    uContexto = zmq.Context()
    uSocketCliente = uContexto.socket(zmq.REP)
    uSocketPar = uContexto.socket(zmq.REP)
    uPublicador = uContexto.socket(zmq.PUB)
    uAssinante = uContexto.socket(zmq.SUB)
    uSocketReferencia = uContexto.socket(zmq.REQ)
    uSocketCliente.setsockopt(zmq.LINGER, 0)
    uSocketPar.setsockopt(zmq.LINGER, 0)
    uPublicador.setsockopt(zmq.LINGER, 0)
    uAssinante.setsockopt(zmq.LINGER, 0)
    uSocketReferencia.setsockopt(zmq.LINGER, 0)
    uSocketReferencia.setsockopt(zmq.RCVTIMEO, 3000)
    uAssinante.setsockopt(zmq.SUBSCRIBE, b"servers")
    uAssinante.setsockopt(zmq.SUBSCRIBE, b"replica")
    uSocketReferencia.connect(uUrlReferencia)
    uRegistro = mSolicitarReferencia(uSocketReferencia, "rank")
    uRank = uRegistro["data"]["rank"]
    uRankServidor = uRank
    uSocketPar.bind("tcp://*:5561")
    uPublicador.connect(os.getenv("PROXY_PUB_URL", "tcp://proxy:5557"))
    uAssinante.connect(uUrlProxyAssinante)
    time.sleep(0.5)
    vAtualizarCoordenador(uSocketReferencia, uPublicador, uAnunciar=True)
    vSolicitarCopiaEstado(uPublicador)
    uFontesEsperadas = {
        uServidor["name"]
        for uServidor in vtServidoresAtivos
        if uServidor.get("name") != uNomeServidor
    }
    uFontesCopia = set()
    uInicioSincronizacao = time.monotonic()
    uPrazoSincronizacao = uInicioSincronizacao + 5
    while time.monotonic() < uPrazoSincronizacao:
        uEsperaMinimaFinalizada = time.monotonic() >= uInicioSincronizacao + 1
        uTodosResponderam = uFontesEsperadas.issubset(uFontesCopia)
        if uEsperaMinimaFinalizada and uTodosResponderam:
            break
        if uAssinante.poll(100) == zmq.POLLIN:
            vReceberMensagemPubSub(uAssinante, uPublicador, uEstado, uFontesCopia)
    uSocketCliente.connect(uUrlBroker)

    uPoller = zmq.Poller()
    uPoller.register(uSocketCliente, zmq.POLLIN)
    uPoller.register(uSocketPar, zmq.POLLIN)
    uPoller.register(uAssinante, zmq.POLLIN)
    print(f"[{uNomeServidor}] rank {uRank}, conectado ao broker", flush=True)
    uPedidosRecebidos = 0

    try:
        while True:
            uEventos = dict(uPoller.poll())
            if uEventos.get(uAssinante) == zmq.POLLIN:
                vReceberMensagemPubSub(uAssinante, uPublicador, uEstado)
            for uSocket in (uSocketCliente, uSocketPar):
                if uEventos.get(uSocket) != zmq.POLLIN:
                    continue

                uPedidoCliente = uSocket is uSocketCliente
                vtMensagemBruta = uSocket.recv()
                uRequisicao = None
                uTamanhosAntigos = None
                try:
                    uRequisicao = msgpack.unpackb(vtMensagemBruta, raw=False)
                    if isinstance(uRequisicao, dict):
                        uTamanhosAntigos = {
                            uNome: len(uEstado[uNome])
                            for uNome in ("users", "logins", "channels", "publications")
                        }
                    if isinstance(uRequisicao, dict) and isinstance(uRequisicao.get("logical_clock"), int):
                        vAtualizarRelogioLogico(uRequisicao["logical_clock"])
                    vExibirMensagem("recebido", uRequisicao)
                    uResultado = mProcessarPedido(uRequisicao, uEstado, uPublicador)
                except (msgpack.UnpackException, ValueError, TypeError) as uErro:
                    uResultado = mCriarResposta(False, f"Mensagem invalida: {uErro}")

                uPedidoInterno = (
                    isinstance(uRequisicao, dict)
                    and uRequisicao.get("op") in ("clock", "election")
                )
                uEventoReplica = None
                if isinstance(uRequisicao, dict) and not uPedidoInterno:
                    uIdEvento = f"{uNomeServidor}:{uuid.uuid4().hex}"
                    if uRequisicao.get("op") == "publish" and uResultado.get("ok"):
                        uResultado["data"]["event_id"] = uIdEvento
                        if len(uEstado["publications"]) > uTamanhosAntigos["publications"]:
                            uEstado["publications"][-1]["event_id"] = uIdEvento

                    uRegistroMensagem = {
                        "event_id": uIdEvento,
                        "request": uRequisicao,
                        "response": uResultado,
                    }
                    uEstado["messages"].append(uRegistroMensagem)
                    uAlteracoes = {
                        uNome: uEstado[uNome][uTamanhosAntigos[uNome]:]
                        for uNome in ("users", "logins", "channels", "publications")
                    }
                    uEventoReplica = {
                        "source": uNomeServidor,
                        "changes": uAlteracoes,
                        "message": uRegistroMensagem,
                    }
                    vSalvarEstado(uEstado)
                vExibirMensagem("enviado", uResultado)
                uSocket.send(msgpack.packb(uResultado, use_bin_type=True))
                if uEventoReplica is not None:
                    vPublicarEventoReplica(uPublicador, uEventoReplica)

                if uPedidoCliente and not uPedidoInterno:
                    uPedidosRecebidos += 1
                if uPedidoCliente and uPedidosRecebidos % uIntervaloHeartbeat == 0:
                    vEnviarHeartbeat(uSocketReferencia, uRank)
                    vAtualizarCoordenador(uSocketReferencia, uPublicador, uAnunciar=True)
                if uPedidoCliente and uPedidosRecebidos % uIntervaloSincronizacao == 0:
                    vSincronizarRelogio(uContexto, uSocketReferencia, uPublicador)
                    vSolicitarCopiaEstado(uPublicador)
    finally:
        uSocketCliente.close()
        uSocketPar.close()
        uPublicador.close()
        uAssinante.close()
        uSocketReferencia.close()
        uContexto.term()


if __name__ == "__main__":
    vExecutarServidor()
