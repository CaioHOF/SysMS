import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
    private static final int TIMEOUT_MS = 10000;

    public static void main(String[] args) {
        String botName = environment("BOT_NAME", "ana");
        String channelName = environment("CHANNEL_NAME", "geral");
        String serverList = environment(
                "SERVER_URLS",
                "tcp://servidor1:5555,tcp://servidor2:5555");

        try {
            for (String endpoint : serverList.split(",")) {
                String server = endpoint.trim();
                if (!server.isEmpty()) {
                    useServer(server, botName, channelName);
                }
            }
        } catch (Exception error) {
            System.err.println("[" + botName + "] falha: " + error.getMessage());
            System.exit(1);
        }
    }

    private static void useServer(String endpoint, String botName, String channelName)
            throws IOException {
        System.out.println("[" + botName + "] conectando a " + endpoint);

        try (ZContext context = new ZContext()) {
            ZMQ.Socket socket = context.createSocket(SocketType.REQ);
            socket.setLinger(0);
            socket.setReceiveTimeOut(TIMEOUT_MS);
            socket.setSendTimeOut(TIMEOUT_MS);
            socket.connect(endpoint);

            Map<String, Object> login = new LinkedHashMap<>();
            login.put("op", "login");
            login.put("username", botName);
            Map<Value, Value> loginReply = send(socket, endpoint, botName, login);
            ensureSuccess(loginReply);

            List<String> channels = listChannels(socket, endpoint, botName, botName);
            if (!channels.contains(channelName)) {
                Map<String, Object> create = new LinkedHashMap<>();
                create.put("op", "create_channel");
                create.put("username", botName);
                create.put("channel", channelName);
                ensureSuccess(send(socket, endpoint, botName, create));
            }

            listChannels(socket, endpoint, botName, botName);
        }
    }

    private static List<String> listChannels(
            ZMQ.Socket socket, String endpoint, String botName, String username)
            throws IOException {
        Map<String, Object> request = new LinkedHashMap<>();
        request.put("op", "list_channels");
        request.put("username", username);

        Map<Value, Value> reply = send(socket, endpoint, botName, request);
        ensureSuccess(reply);

        MapValue data = field(reply, "data").asMapValue();
        ArrayValue values = field(data.map(), "channels").asArrayValue();
        List<String> channels = new ArrayList<>();
        for (Value value : values) {
            channels.add(value.asStringValue().asString());
        }
        return channels;
    }

    private static Map<Value, Value> send(
            ZMQ.Socket socket,
            String endpoint,
            String botName,
            Map<String, Object> request) throws IOException {
        request.put("timestamp", Instant.now().toEpochMilli());
        System.out.println("[" + botName + " -> " + endpoint + "] enviado: " + request);

        socket.send(pack(request), 0);
        byte[] bytes = socket.recv(0);
        if (bytes == null) {
            throw new IOException("o servidor não respondeu dentro do prazo");
        }

        Value message = MessagePack.newDefaultUnpacker(bytes).unpackValue();
        if (!message.isMapValue()) {
            throw new IOException("a resposta do servidor não é um mapa MessagePack");
        }
        Map<Value, Value> reply = message.asMapValue().map();
        System.out.println("[" + endpoint + " -> " + botName + "] recebido: " + message);
        return reply;
    }

    private static byte[] pack(Map<String, Object> message) throws IOException {
        MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
        packer.packMapHeader(message.size());
        for (Map.Entry<String, Object> item : message.entrySet()) {
            packer.packString(item.getKey());
            Object value = item.getValue();
            if (value instanceof String text) {
                packer.packString(text);
            } else if (value instanceof Long number) {
                packer.packLong(number);
            } else {
                throw new IOException("tipo de campo não suportado: " + item.getKey());
            }
        }
        return packer.toByteArray();
    }

    private static void ensureSuccess(Map<Value, Value> reply) throws IOException {
        Value ok = field(reply, "ok");
        if (!ok.isBooleanValue() || !ok.asBooleanValue().getBoolean()) {
            Value message = reply.get(ValueFactory.newString("message"));
            throw new IOException(message == null ? "operação recusada" : message.toString());
        }
    }

    private static Value field(Map<Value, Value> map, String name) throws IOException {
        Value value = map.get(ValueFactory.newString(name));
        if (value == null || value.isNilValue()) {
            throw new IOException("campo ausente na resposta: " + name);
        }
        return value;
    }

    private static String environment(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
