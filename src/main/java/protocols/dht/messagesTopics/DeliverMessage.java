package protocols.dht.messagesTopics;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;

import java.net.UnknownHostException;

public class DeliverMessage extends ProtocolMessage {

    public static short MSG_CODE = 10227;

    private String topic;
    private String message;

    public DeliverMessage(String topic, String message) {
        super(MSG_CODE);
        this.topic = topic;
        this.message = message;
    }

    public static final ISerializer<DeliverMessage> serializer = new ISerializer<DeliverMessage>() {
        @Override
        public void serialize(DeliverMessage m, ByteBuf out) {
            out.writeInt(m.topic.length());
            out.writeBytes(m.topic.getBytes());
            out.writeInt(m.message.length());
            out.writeBytes(m.message.getBytes());
        }

        @Override
        public DeliverMessage deserialize(ByteBuf in) {
            byte[] topicBytes = new byte[in.readInt()];
            in.readBytes(topicBytes);
            String topic = new String(topicBytes);

            byte[] messageBytes = new byte[in.readInt()];
            in.readBytes(messageBytes);
            String message = new String(topicBytes);

            return new DeliverMessage(topic, message);
        }

        @Override
        public int serializedSize(DeliverMessage m) {
            return 2 * Integer.BYTES + m.topic.length() + m.message.length();
        }
    };
}
