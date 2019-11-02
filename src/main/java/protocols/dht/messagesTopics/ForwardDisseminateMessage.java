package protocols.dht.messagesTopics;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;

import java.net.UnknownHostException;

public class ForwardDisseminateMessage extends ProtocolMessage {

    public static short MSG_CODE = 10727;

    private String topic;
    private String message;

    public ForwardDisseminateMessage(String topic, String message) {
        super(MSG_CODE);
        this.topic = topic;
        this.message = message;
    }

    public static final ISerializer<ForwardDisseminateMessage> serializer = new ISerializer<ForwardDisseminateMessage>() {
        @Override
        public void serialize(ForwardDisseminateMessage m, ByteBuf out) {
            out.writeInt(m.topic.length());
            out.writeBytes(m.topic.getBytes());
            out.writeInt(m.message.length());
            out.writeBytes(m.message.getBytes());
        }

        @Override
        public ForwardDisseminateMessage deserialize(ByteBuf in) {
            byte[] topicBytes = new byte[in.readInt()];
            in.readBytes(topicBytes);
            String topic = new String(topicBytes);

            byte[] messageBytes = new byte[in.readInt()];
            in.readBytes(messageBytes);
            String message = new String(topicBytes);

            return new ForwardDisseminateMessage(topic, message);
        }

        @Override
        public int serializedSize(ForwardDisseminateMessage m) {
            return 2 * Integer.BYTES + m.topic.length() + m.message.length();
        }
    };

    public String getTopic() {
        return topic;
    }

    public String getMessage() {
        return message;
    }
}
