package protocols.dissemination.message;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.Host;
import network.ISerializer;
import protocols.dissemination.MessageType;

import java.net.UnknownHostException;

public class DeliverMessage extends ProtocolMessage {

    public static short MSG_CODE = 16427;

    private String topic;
    private String message;
    private MessageType messageType;
    private Host host;

    private DeliverMessage() {
        super(MSG_CODE);
    }

    public DeliverMessage(String topic, String message) {
        super(MSG_CODE);
        this.topic = topic;
        this.message = message;
        this.messageType = MessageType.PUBLICATION;
    }

    public DeliverMessage(String topic,boolean toSubscribe,Host host) {
        super(MSG_CODE);
        this.topic = topic;
        this.host = host;
        this.messageType = toSubscribe? MessageType.SUBSCRIBE : MessageType.UNSUBSCRIBE;
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

    private static final ISerializer<DeliverMessage> serializerPublication = new ISerializer<DeliverMessage>() {
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
            String message = new String(messageBytes);

            return new DeliverMessage(topic, message);
        }

        @Override
        public int serializedSize(DeliverMessage m) {
            return 2 * Integer.BYTES + m.topic.length() + m.message.length();
        }
    };

    private static final ISerializer<DeliverMessage> serializersSubscription = new ISerializer<DeliverMessage>() {
        @Override
        public void serialize(DeliverMessage m, ByteBuf out) {
            out.writeInt(m.topic.length());
            out.writeBytes(m.topic.getBytes());
            m.getHost().serialize(out);
        }

        @Override
        public DeliverMessage deserialize(ByteBuf in) throws UnknownHostException {
            byte[] topicBytes = new byte[in.readInt()];
            in.readBytes(topicBytes);
            String topic = new String(topicBytes);

            DeliverMessage deliverMessage = new DeliverMessage();

            deliverMessage.setTopic(topic);
            deliverMessage.setHost(Host.deserialize(in));
            return deliverMessage;
        }

        @Override
        public int serializedSize(DeliverMessage m) {
            return Integer.BYTES + m.topic.length() + 6;
        }
    };

    public static final ISerializer<DeliverMessage> serializer = new ISerializer<DeliverMessage>() {
        @Override
        public void serialize(DeliverMessage m, ByteBuf out) {
            out.writeInt(m.messageType.toString().length());
            out.writeBytes(m.messageType.toString().getBytes());
            if(m.messageType.equals(MessageType.PUBLICATION)){
                serializerPublication.serialize(m,out);
            }else{
                serializersSubscription.serialize(m,out);
            }
        }

        @Override
        public DeliverMessage deserialize(ByteBuf in) throws UnknownHostException {
            byte[] messageTypeBytes = new byte[in.readInt()];
            in.readBytes(messageTypeBytes);
            MessageType messageType = MessageType.valueOf(new String(messageTypeBytes));

            DeliverMessage message;
            if(messageType.equals(MessageType.PUBLICATION)){
                message = serializerPublication.deserialize(in);
            }else{
                message = serializersSubscription.deserialize(in);
            }
            message.setMessageType(messageType);

            return message;
        }

        @Override
        public int serializedSize(DeliverMessage m) {
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
}
