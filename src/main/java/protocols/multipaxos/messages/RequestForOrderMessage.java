package protocols.multipaxos.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;

import java.util.LinkedList;
import java.util.List;

public class RequestForOrderMessage extends ProtocolMessage {
    public final static short MSG_CODE = 20005;
    public static final ISerializer<RequestForOrderMessage> serializer = new ISerializer<RequestForOrderMessage>() {
        @Override
        public void serialize(RequestForOrderMessage m, ByteBuf out) {
            out.writeInt(m.getMessages().size());
            for (String message : m.getMessages()) {
                out.writeInt(message.length());
                out.readBytes(message.getBytes());
            }

            out.writeInt(m.topic.length());
            out.writeBytes(m.topic.getBytes());
        }

        @Override
        public RequestForOrderMessage deserialize(ByteBuf in) {
            int size = in.readInt();
            List<String> messages = new LinkedList<>();

            for (int i = 0; i < size; i++) {
                byte[] msgBytes = new byte[in.readInt()];
                messages.add(new String(msgBytes));
            }

            byte[] topicBytes = new byte[in.readInt()];
            in.readBytes(topicBytes);

            return new RequestForOrderMessage(new String(topicBytes), messages);
        }

        @Override
        public int serializedSize(RequestForOrderMessage m) {
            int size = 2 * Integer.BYTES + m.topic.length();
            for (String msg : m.getMessages()) {
                size += msg.length() + Integer.BYTES;
            }

            return size;
        }
    };
    private String topic;
    private List<String> messages;

    public RequestForOrderMessage(String topic, List<String> messages) {
        super(MSG_CODE);
        this.topic = topic;
        this.messages = messages;
    }

    public String getTopic() {
        return topic;
    }

    public List<String> getMessages() {
        return messages;
    }

}
