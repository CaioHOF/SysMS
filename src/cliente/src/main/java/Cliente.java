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
    private static final int TIMEOUT_MS = 10000;
    private static final Random random = new Random();
    private static final AtomicLong logicalClock = new AtomicLong();

    public static void main(String[] args) {
        String username = environment("BOT_NAME", "ana");
        String proxyUrl = environment("PROXY_SUB_URL", "tcp://proxy:5558");
        String[] addresses = environment("SERVER_URLS",
                "tcp://servidor1:5555,tcp://servidor2:5555").split(",");

        try (ZContext context = new ZContext()) {
            Map<String, ZMQ.Socket> servers = new LinkedHashMap<>();
            List<String> channels = new ArrayList<>();

            for (String address : addresses) {
                String endpoint = address.trim();
                ZMQ.Socket socket = context.createSocket(SocketType.REQ);
                socket.setLinger(0);
                socket.setReceiveTimeOut(TIMEOUT_MS);
                socket.setSendTimeOut(TIMEOUT_MS);
                socket.connect(endpoint);
                servers.put(endpoint, socket);

                request(socket, endpoint, username, "login", null, null);
                channels = listChannels(socket, endpoint, username);
                while (channels.size() < 5) {
                    int number = 1;
                    while (channels.contains("canal-" + number)) {
                        number++;
                    }
                    request(socket, endpoint, username, "create_channel",
                            "canal-" + number, null);
                    channels = listChannels(socket, endpoint, username);
                }
            }

            Collections.shuffle(channels, random);
            List<String> subscriptions = new ArrayList<>(
                    channels.subList(0, Math.min(3, channels.size())));
            System.out.println("[" + username + "] inscritos em: " + subscriptions);

            CountDownLatch subscribed = new CountDownLatch(1);
            Thread listener = new Thread(() -> receiveMessages(
                    proxyUrl, subscriptions, username, subscribed));
            listener.setDaemon(true);
            listener.start();
            subscribed.await();
            Thread.sleep(1000);

            List<Map.Entry<String, ZMQ.Socket>> serverList = new ArrayList<>(servers.entrySet());
            long counter = 1;
            while (true) {
                for (int i = 0; i < 10; i++) {
                    String channel = channels.get(random.nextInt(channels.size()));
                    String text = "mensagem-" + random.nextInt(1000);
                    Map.Entry<String, ZMQ.Socket> server =
                            serverList.get(random.nextInt(serverList.size()));
                    request(server.getValue(), server.getKey(), username,
                            "publish", channel, text);
                    System.out.println("[" + username + "] enviada " + counter++
                            + " em #" + channel);
                    Thread.sleep(1000);
                }
            }
        } catch (Exception error) {
            System.err.println("Falha no bot: " + error.getMessage());
            System.exit(1);
        }
    }

    private static List<String> listChannels(
            ZMQ.Socket socket, String endpoint, String username) throws IOException {
        Map<Value, Value> reply = request(
                socket, endpoint, username, "list_channels", null, null);
        MapValue data = field(reply, "data").asMapValue();
        ArrayValue values = field(data.map(), "channels").asArrayValue();
        List<String> channels = new ArrayList<>();
        for (Value value : values) {
            channels.add(value.asStringValue().asString());
        }
        return channels;
    }

    private static Map<Value, Value> request(
            ZMQ.Socket socket, String endpoint, String username, String operation,
            String channel, String text) throws IOException {
        Map<String, Object> message = new LinkedHashMap<>();
        message.put("op", operation);
        message.put("username", username);
        if (channel != null) message.put("channel", channel);
        if (text != null) message.put("message", text);
        message.put("timestamp", Instant.now().toEpochMilli());
        message.put("logical_clock", logicalClock.incrementAndGet());

        System.out.println("[" + username + " -> " + endpoint + "] enviado: " + message);
        socket.send(pack(message), 0);
        byte[] bytes = socket.recv(0);
        if (bytes == null) throw new IOException("o servidor não respondeu");

        Value response = MessagePack.newDefaultUnpacker(bytes).unpackValue();
        Map<Value, Value> fields = response.asMapValue().map();
        long receivedClock = field(fields, "logical_clock").asIntegerValue().asLong();
        logicalClock.updateAndGet(current -> Math.max(current, receivedClock));
        System.out.println("[" + endpoint + " -> " + username + "] recebido: " + response);
        if (!field(fields, "ok").asBooleanValue().getBoolean()) {
            throw new IOException(field(fields, "message").toString());
        }
        return fields;
    }

    private static void receiveMessages(
            String proxyUrl, List<String> channels, String username, CountDownLatch ready) {
        try (ZContext context = new ZContext()) {
            ZMQ.Socket subscriber = context.createSocket(SocketType.SUB);
            subscriber.setLinger(0);
            subscriber.connect(proxyUrl);
            for (String channel : channels) {
                subscriber.subscribe((channel + "\0").getBytes(java.nio.charset.StandardCharsets.UTF_8));
            }
            ready.countDown();

            while (true) {
                subscriber.recv(0); //Se der erro o problema ta aqui
                byte[] payload = subscriber.recv(0);
                Map<Value, Value> publication = MessagePack.newDefaultUnpacker(payload)
                        .unpackValue().asMapValue().map();
                long receivedClock = field(publication, "logical_clock").asIntegerValue().asLong();
                logicalClock.updateAndGet(current -> Math.max(current, receivedClock));
                long receivedAt = Instant.now().toEpochMilli();
                System.out.println("[" + username + " recebeu] canal=#"
                        + field(publication, "channel").asStringValue().asString()
                        + " mensagem=" + field(publication, "message").asStringValue().asString()
                        + " enviada=" + field(publication, "sent_at").asIntegerValue().asLong()
                        + " recebida=" + receivedAt);
            }
        } catch (Exception error) {
            System.err.println("Falha ao receber publicação: " + error.getMessage());
            ready.countDown();
        }
    }

    private static byte[] pack(Map<String, Object> message) throws IOException {
        MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
        packer.packMapHeader(message.size());
        for (Map.Entry<String, Object> item : message.entrySet()) {
            packer.packString(item.getKey());
            if (item.getValue() instanceof String text) packer.packString(text);
            else if (item.getValue() instanceof Long number) packer.packLong(number);
        }
        return packer.toByteArray();
    }

    private static Value field(Map<Value, Value> map, String name) throws IOException {
        Value value = map.get(ValueFactory.newString(name));
        if (value == null || value.isNilValue()) throw new IOException("campo ausente: " + name);
        return value;
    }

    private static String environment(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}