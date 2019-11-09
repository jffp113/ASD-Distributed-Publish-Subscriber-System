package protocols.dht.messagesTopics;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.Host;
import network.ISerializer;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class RefreshTopicsMessage extends ProtocolMessage {

    public static short MSG_CODE = 1017;

    private Map<String, Set<Host>> topics;

    public RefreshTopicsMessage(Map<String, Set<Host>> topics) {
        super(MSG_CODE);
        this.topics = topics;
    }

    public Map<String, Set<Host>> getTopics() {
        return topics;
    }

    public static final ISerializer<RefreshTopicsMessage> serializer = new ISerializer<RefreshTopicsMessage>() {
        @Override
        public void serialize(RefreshTopicsMessage m, ByteBuf out) {
            out.writeInt(m.topics.size());
            for (Map.Entry<String, Set<Host>> entry : m.topics.entrySet()) {
                String topic = entry.getKey();
                out.writeInt(topic.length());
                out.writeBytes(topic.getBytes());

                Set<Host> subscribers = entry.getValue();
                out.writeInt(subscribers.size());
                for (Host h : subscribers) {
                    h.serialize(out);
                }

            }
        }

        @Override
        public RefreshTopicsMessage deserialize(ByteBuf in) throws UnknownHostException {
            int numTopics = in.readInt();
            Map<String, Set<Host>> topics = new HashMap<>();
            for (int i = 0; i < numTopics; i++) {
                byte[] topicBytes = new byte[in.readInt()];
                in.readBytes(topicBytes);
                String topic = new String(topicBytes);

                int numSubscribers = in.readInt();
                Set<Host> subscribers = new HashSet<>();

                for (int j = 0; j < numSubscribers; j++) {
                    subscribers.add(Host.deserialize(in));
                }

                topics.put(topic, subscribers);
            }

            return new RefreshTopicsMessage(topics);
        }

        @Override
        public int serializedSize(RefreshTopicsMessage m) {
            int topicsAccSize = 0;
            int setAccSize = 0;

            for (Map.Entry<String, Set<Host>> entry : m.topics.entrySet()) {
                topicsAccSize += entry.getKey().length() + Integer.BYTES;
                setAccSize += entry.getValue().size();
            }

            return Integer.BYTES + setAccSize * (6 + Integer.BYTES) + topicsAccSize;
        }
    };
}
