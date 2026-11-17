import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.MapValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class Cliente {
    private static final int uTempoLimiteMs = 10000;
    private static final Random uAleatorio = new Random();
    // Contador usado para sincronizar, é compartilhado pelas operações do cliente
    private static final AtomicLong uRelogioLogico = new AtomicLong();

    public static void main(String[] vtArgumentos) {
        vExecutarBot();
    }

    private static void vExecutarBot() {
        // Essas tres variaveis servem para ler o ambiente e definir endereços
        String uNomeUsuario = sLerVariavelAmbiente("BOT_NAME", "ana");
        String uUrlBroker = sLerVariavelAmbiente("BROKER_URL", "tcp://broker:5555");
        String uUrlProxy = sLerVariavelAmbiente("PROXY_SUB_URL", "tcp://proxy:5558");

        //Cria o contexto
        try (ZContext uContexto = new ZContext()) {
            List<String> vtCanais = new ArrayList<>();

            ZMQ.Socket uSocketServidor = uContexto.createSocket(SocketType.REQ);
            uSocketServidor.setLinger(0);
            uSocketServidor.setReceiveTimeOut(uTempoLimiteMs);
            uSocketServidor.setSendTimeOut(uTempoLimiteMs);
            uSocketServidor.connect(uUrlBroker);

            //Literalmebnte envia o pedido de login do bot do ambiente
            mEnviarPedido(uSocketServidor, uUrlBroker, uNomeUsuario, "login", null, null);
            vtCanais = vtListarCanais(uSocketServidor, uUrlBroker, uNomeUsuario);

            //continua criando até ter 5
            while (vtCanais.size() < 5) {

                //procura um nome de canal que nao exista
                int uNumeroCanal = 1;
                while (vtCanais.contains("canal-" + uNumeroCanal)) {
                    uNumeroCanal++;
                }

                //cria o canal
                mEnviarPedido(uSocketServidor, uUrlBroker, uNomeUsuario, "create_channel",
                        "canal-" + uNumeroCanal, null);
                vtCanais = vtListarCanais(uSocketServidor, uUrlBroker, uNomeUsuario);
            }

            //Para variair os canais escolhidos para inscrição embaralho os canais
            Collections.shuffle(vtCanais, uAleatorio);
            //3 canais são selecionados para receber publicações, antes eu tava selecionando 4, mas tava muito ruim 
            List<String> vtInscricoes = new ArrayList<>(
                    vtCanais.subList(0, Math.min(3, vtCanais.size())));
            System.out.println("[" + uNomeUsuario + "] inscritos em: " + vtInscricoes);

            CountDownLatch uAvisoInscricao = new CountDownLatch(1);
            // cria uma thread separada para receber mensagens dos canais inscritos, se n receber a mensagem o problema ta aqui
            Thread uThreadRecebedora = new Thread(() -> vReceberMensagens(
                    uUrlProxy, vtInscricoes, uNomeUsuario, uAvisoInscricao));
            uThreadRecebedora.setDaemon(true);
            uThreadRecebedora.start();
            uAvisoInscricao.await();
            Thread.sleep(1000);

            long uContadorMensagens = 1;
            //mantem o funcionamento até ser encerrado
            while (true) {
                for (int uIndice = 0; uIndice < 10; uIndice++) {
                    String uCanal = vtCanais.get(uAleatorio.nextInt(vtCanais.size()));
                    String uTexto = "mensagem-" + uAleatorio.nextInt(1000);
                    mEnviarPedido(uSocketServidor, uUrlBroker, uNomeUsuario,
                            "publish", uCanal, uTexto);
                    System.out.println("[" + uNomeUsuario + "] enviada " + uContadorMensagens++
                            + " em #" + uCanal);
                    Thread.sleep(1000);
                }
            }
        } catch (Exception uErro) {
            System.err.println("Falha no seguinte bot: " + uErro.getMessage());
            System.exit(1);
        }
    }

    private static List<String> vtListarCanais(
            ZMQ.Socket uSocket, String uEndereco, String uNomeUsuario) throws IOException {
        Map<Value, Value> uResposta = mEnviarPedido(
                uSocket, uEndereco, uNomeUsuario, "list_channels", null, null);
        MapValue uDados = valObterCampo(uResposta, "data").asMapValue();
        ArrayValue vtValores = valObterCampo(uDados.map(), "channels").asArrayValue();
        List<String> vtCanais = new ArrayList<>();
        for (Value uValor : vtValores) {
            vtCanais.add(uValor.asStringValue().asString());
        }
        return vtCanais;
    }

    //Faz todo processo de um pedido(monta, envia e processa)
    private static Map<Value, Value> mEnviarPedido(
            ZMQ.Socket uSocket, String uEndereco, String uNomeUsuario, String uOperacao,
            String uCanal, String uTexto) throws IOException {
        Map<String, Object> uMensagem = new LinkedHashMap<>();
        uMensagem.put("op", uOperacao);
        uMensagem.put("username", uNomeUsuario);
        if (uCanal != null) uMensagem.put("channel", uCanal);
        if (uTexto != null) uMensagem.put("message", uTexto);
        // coloca o horario atual em milissegundos
        uMensagem.put("timestamp", Instant.now().toEpochMilli());
        // incrementa e inclui o relógio lógico do cliente
        uMensagem.put("logical_clock", uRelogioLogico.incrementAndGet());

        System.out.println("[" + uNomeUsuario + " -> " + uEndereco + "] enviado: " + uMensagem);
        uSocket.send(vtEmpacotarMensagem(uMensagem), 0);
        byte[] vtBytesResposta = uSocket.recv(0);
        if (vtBytesResposta == null) throw new IOException("o servidor não respondeu");

        Value uResposta = MessagePack.newDefaultUnpacker(vtBytesResposta).unpackValue();
        Map<Value, Value> uCampos = uResposta.asMapValue().map();
        long uRelogioRecebido = valObterCampo(uCampos, "logical_clock").asIntegerValue().asLong();
        uRelogioLogico.updateAndGet(uAtual -> Math.max(uAtual, uRelogioRecebido));
        System.out.println("[" + uEndereco + " -> " + uNomeUsuario + "] recebido: " + uResposta);
        if (!valObterCampo(uCampos, "ok").asBooleanValue().getBoolean()) {
            throw new IOException(valObterCampo(uCampos, "message").toString());
        }
        return uCampos;
    }
    // recebe em uma thread separada as publicações dos canais inscritos
    private static void vReceberMensagens(
            String uUrlProxy, List<String> vtCanais, String uNomeUsuario, CountDownLatch uSinalPronto) {
        try (ZContext uContexto = new ZContext()) {
            ZMQ.Socket uSocketAssinante = uContexto.createSocket(SocketType.SUB);
            uSocketAssinante.setLinger(0);
            uSocketAssinante.connect(uUrlProxy);
            //inscreve e assina mensagens que o topico que o bot deveria se inscrever
            for (String uCanal : vtCanais) {
                uSocketAssinante.subscribe((uCanal + "\0").getBytes(java.nio.charset.StandardCharsets.UTF_8));
            }
            // avisa a thread principal que as inscrições foram configuradas
            uSinalPronto.countDown();

            //faz o bot continuar recebendo mensagem até ser interrompido
            while (true) {
                //topico
                uSocketAssinante.recv(0);
                //contem os dados da publicacao
                byte[] vtConteudo = uSocketAssinante.recv(0);
                Map<Value, Value> uPublicacao = MessagePack.newDefaultUnpacker(vtConteudo)
                        .unpackValue().asMapValue().map();
                long uRelogioRecebido = valObterCampo(uPublicacao, "logical_clock").asIntegerValue().asLong();
                uRelogioLogico.updateAndGet(uAtual -> Math.max(uAtual, uRelogioRecebido));
                long uHorarioRecebido = Instant.now().toEpochMilli();
                // exibe quem enviou a mensagem e o canal em que foi publicada
                System.out.println("[" + uNomeUsuario + " recebeu] remetente="
                        + valObterCampo(uPublicacao, "username").asStringValue().asString()
                        + " canal=#" + valObterCampo(uPublicacao, "channel").asStringValue().asString()
                        + " mensagem=" + valObterCampo(uPublicacao, "message").asStringValue().asString()
                        + " timestamp=" + valObterCampo(uPublicacao, "timestamp").asIntegerValue().asLong()
                        + " enviada=" + valObterCampo(uPublicacao, "sent_at").asIntegerValue().asLong()
                        + " recebida_servidor="
                        + valObterCampo(uPublicacao, "received_at").asIntegerValue().asLong()
                        + " relogio_logico=" + uRelogioRecebido
                        + " recebida_cliente=" + uHorarioRecebido);
            }
        } catch (Exception uErro) {
            System.err.println("Falha ao receber publicação: " + uErro.getMessage());
            uSinalPronto.countDown();
        }
    }
    // converte o mapa Java para bytes
    private static byte[] vtEmpacotarMensagem(Map<String, Object> uMensagem) throws IOException {
        MessageBufferPacker uEmpacotador = MessagePack.newDefaultBufferPacker();
        uEmpacotador.packMapHeader(uMensagem.size());
        for (Map.Entry<String, Object> uItem : uMensagem.entrySet()) {
            uEmpacotador.packString(uItem.getKey());
            if (uItem.getValue() instanceof String uTexto) uEmpacotador.packString(uTexto);
            else if (uItem.getValue() instanceof Long uNumeroCanal) uEmpacotador.packLong(uNumeroCanal);
        }
        return uEmpacotador.toByteArray();
    }
    // Procura um campo no mapa MessagePack e devolve seu valor
    private static Value valObterCampo(Map<Value, Value> uMapa, String uNomeCampo) throws IOException {
        Value uValor = uMapa.get(ValueFactory.newString(uNomeCampo));
        if (uValor == null || uValor.isNilValue()) throw new IOException("campo ausente: " + uNomeCampo);
        return uValor;
    }
    //só le as variaveis do ambiente
    private static String sLerVariavelAmbiente(String uNomeCampo, String uValorPadrao) {
        String uValor = System.getenv(uNomeCampo);
        return uValor == null || uValor.isBlank() ? uValorPadrao : uValor;
    }
}
