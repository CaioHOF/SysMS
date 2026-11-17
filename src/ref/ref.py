import time

import msgpack
import zmq


uServidoresRegistrados = {}
uUltimoHeartbeat = {}
uProximoRank = 1
uRelogioLogico = 0
uTimeoutHeartbeat = 30


def iObterTimestamp():
    return int(time.time() * 1000)


def mCriarResposta(uSucesso, uMensagemResposta, uDados=None):
    #coloquei global pra facilitar
    global uRelogioLogico
    uRelogioLogico += 1
    uResultado = {
        "ok": uSucesso,
        "timestamp": iObterTimestamp(),
        "logical_clock": uRelogioLogico,
        "message": uMensagemResposta,
    }
    if uDados is not None:
        uResultado["data"] = uDados
    return uResultado


def vtListarServidoresAtivos():
    uAgora = time.monotonic()
    for uNome, uUltimoSinal in list(uUltimoHeartbeat.items()):
        if uAgora - uUltimoSinal > uTimeoutHeartbeat:
            del uUltimoHeartbeat[uNome]
    return sorted(
        [{"name": uNome, "rank": uRank} for uNome, uRank in uServidoresRegistrados.items()
         if uNome in uUltimoHeartbeat],
        key=lambda uServidor: uServidor["rank"],
    )


def mProcessarPedido(uRequisicao):
    global uRelogioLogico, uProximoRank
    if not isinstance(uRequisicao, dict):
        return mCriarResposta(False, "A mensagem precisa ser um mapa MessagePack")
    if not isinstance(uRequisicao.get("timestamp"), int):
        return mCriarResposta(False, "A mensagem precisa conter timestamp")
    if not isinstance(uRequisicao.get("logical_clock"), int):
        return mCriarResposta(False, "A mensagem precisa conter contador lógico")

    uRelogioLogico = max(uRelogioLogico, uRequisicao["logical_clock"])
    uOperacao = uRequisicao.get("op")
    uNome = uRequisicao.get("name")

    if uOperacao == "list":
        return mCriarResposta(True, "Servidores disponíveis", {"servers": vtListarServidoresAtivos()})

    if not isinstance(uNome, str) or not uNome:
        return mCriarResposta(False, "Nome do servidor inválido")

    if uOperacao == "rank":
        if uNome not in uServidoresRegistrados:
            uServidoresRegistrados[uNome] = uProximoRank
            uProximoRank += 1
        uUltimoHeartbeat[uNome] = time.monotonic()
        return mCriarResposta(True, "Rank do servidor", {"rank": uServidoresRegistrados[uNome]})

    if uOperacao == "heartbeat":
        if uNome not in uServidoresRegistrados or uRequisicao.get("rank") != uServidoresRegistrados[uNome]:
            return mCriarResposta(False, "Servidor não registrado")
        uUltimoHeartbeat[uNome] = time.monotonic()
        return mCriarResposta(True, "Heartbeat recebido", {"rank": uServidoresRegistrados[uNome]})

    return mCriarResposta(False, "Operação desconhecida")


def vExecutarRef():
    uContexto = zmq.Context()
    uSocket = uContexto.socket(zmq.REP)
    uSocket.setsockopt(zmq.LINGER, 0)
    uSocket.bind("tcp://*:5560")
    print("Serviço ref aguardando na porta 5560", flush=True)

    try:
        while True:
            vtMensagemBruta = uSocket.recv()
            try:
                uRequisicao = msgpack.unpackb(vtMensagemBruta, raw=False)
                print("ref recebeu:", uRequisicao, flush=True)
                uResultado = mProcessarPedido(uRequisicao)
            except (msgpack.UnpackException, ValueError, TypeError) as uErro:
                uResultado = mCriarResposta(False, f"Mensagem inválida: {uErro}")
            print("ref enviou:", uResultado, flush=True)
            uSocket.send(msgpack.packb(uResultado, use_bin_type=True))
    finally:
        uSocket.close()
        uContexto.term()


if __name__ == "__main__":
    vExecutarRef()
