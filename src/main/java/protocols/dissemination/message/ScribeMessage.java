package protocols.dissemination.message;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.Host;
import network.ISerializer;
import protocols.dissemination.MessageType;

import java.net.UnknownHostException;

public class ScribeMessage extends ProtocolMessage {

    public static short MSG_CODE = 16427;

    private String topic;
    private String message;
    private MessageType messageType;
    private Host host;
    private int seq;

    private ScribeMessage() {
        super(MSG_CODE);
    }

    public ScribeMessage(String topic, String message, Host host, int seq) {
        super(MSG_CODE);
        this.topic = topic;
        this.message = message;
        this.host=host;
        this.messageType = MessageType.PUBLICATION;
        this.seq = seq;

    }

    public ScribeMessage(String topic, boolean subscribe, Host host) {
        super(MSG_CODE);
        this.topic = topic;
        this.host = host;
        this.messageType = subscribe ? MessageType.SUBSCRIBE : MessageType.UNSUBSCRIBE;
    }

    public void setHost(Host host) {
        this.host = host;
    }

    private void setTopic(String topic) {
        this.topic = topic;
    }

    private void setMessageType(MessageType messageType) {
        this.messageType = messageType;
    }

    private static final ISerializer<ScribeMessage> serializerPublication = new ISerializer<ScribeMessage>() {
        @Override
        public void serialize(ScribeMessage m, ByteBuf out) {
            out.writeInt(m.topic.length());
            out.writeBytes(m.topic.getBytes());
            out.writeInt(m.message.length());
            out.writeBytes(m.message.getBytes());
            m.host.serialize(out);
            out.writeInt(m.seq);
        }

        @Override
        public ScribeMessage deserialize(ByteBuf in) throws UnknownHostException {
            byte[] topicBytes = new byte[in.readInt()];
            in.readBytes(topicBytes);
            String topic = new String(topicBytes);

            byte[] messageBytes = new byte[in.readInt()];
            in.readBytes(messageBytes);
            String message = new String(messageBytes);

            return new ScribeMessage(topic, message,Host.deserialize(in), in.readInt());
        }

        @Override
        public int serializedSize(ScribeMessage m) {
            return 3 * Integer.BYTES + m.topic.length() + m.message.length()+m.host.serializedSize();
        }
    };

    private static final ISerializer<ScribeMessage> serializersSubscription = new ISerializer<ScribeMessage>() {
        @Override
        public void serialize(ScribeMessage m, ByteBuf out) {
            out.writeInt(m.topic.length());
            out.writeBytes(m.topic.getBytes());
            m.getHost().serialize(out);
        }

        @Override
        public ScribeMessage deserialize(ByteBuf in) throws UnknownHostException {
            byte[] topicBytes = new byte[in.readInt()];
            in.readBytes(topicBytes);
            String topic = new String(topicBytes);

            ScribeMessage deliverMessage = new ScribeMessage();

            deliverMessage.setTopic(topic);
            deliverMessage.setHost(Host.deserialize(in));
            return deliverMessage;
        }

        @Override
        public int serializedSize(ScribeMessage m) {
            return Integer.BYTES + m.topic.length() + 6;
        }
    };

    public static final ISerializer<ScribeMessage> serializer = new ISerializer<ScribeMessage>() {
        @Override
        public void serialize(ScribeMessage m, ByteBuf out) {
            out.writeInt(m.messageType.toString().length());
            out.writeBytes(m.messageType.toString().getBytes());
            if (m.messageType.equals(MessageType.PUBLICATION)) {
                serializerPublication.serialize(m, out);
            } else {
                serializersSubscription.serialize(m, out);
            }
        }

        @Override
        public ScribeMessage deserialize(ByteBuf in) throws UnknownHostException {
            byte[] messageTypeBytes = new byte[in.readInt()];
            in.readBytes(messageTypeBytes);
            MessageType messageType = MessageType.valueOf(new String(messageTypeBytes));

            ScribeMessage message;
            if (messageType.equals(MessageType.PUBLICATION)) {
                message = serializerPublication.deserialize(in);
            } else {
                message = serializersSubscription.deserialize(in);
            }
            message.setMessageType(messageType);

            return message;
        }

        @Override
        public int serializedSize(ScribeMessage m) {
            return Integer.BYTES + m.messageType.toString().length() +
                    (m.getMessageType().equals(MessageType.PUBLICATION) ? serializerPublication.serializedSize(m) :
                            serializersSubscription.serializedSize(m));
        }
    };


    public String getTopic() {
        return topic;
    }

    public String getMessage() {
        return message;
    }

    public Host getHost() {
        return host;
    }

    public MessageType getMessageType() {
        return messageType;
    }

    @Override
    public String toString() {
        return "[" + topic + "," + message + "," + messageType + "]";
    }

    public int getSeq() {
        return seq;
    }
}
