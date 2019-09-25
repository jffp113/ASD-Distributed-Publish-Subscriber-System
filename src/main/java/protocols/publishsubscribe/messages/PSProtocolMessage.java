package protocols.publishsubscribe.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;

import java.net.UnknownHostException;
import java.util.UUID;

public class PSProtocolMessage extends ProtocolMessage {

    public final static short MSG_CODE = 102;
    private UUID mid;
    private String message;
    private String topic;
    private volatile int size = -1;

    public static final ISerializer<PSProtocolMessage> serializer = new ISerializer<PSProtocolMessage>() {
        @Override
        public void serialize(PSProtocolMessage PSProtocolMessage, ByteBuf out) {
            out.writeShort(PSProtocolMessage.topic.length());
            out.writeBytes(PSProtocolMessage.topic.getBytes());
            out.writeShort(PSProtocolMessage.message.length());
            out.writeBytes(PSProtocolMessage.message.getBytes());
        }

        @Override
        public PSProtocolMessage deserialize(ByteBuf in) throws UnknownHostException {
            short topicSize = in.readShort();
            byte[] topic = new byte[topicSize];
            in.readBytes(topic);
            short messageSize = in.readShort();
            byte[] message = new byte[messageSize];
            in.readBytes(message);

            return new PSProtocolMessage(new String(message), new String(topic));
        }

        @Override
        public int serializedSize(PSProtocolMessage PSProtocolMessage) {
            return PSProtocolMessage.size;
        }
    };

    public PSProtocolMessage(String message, String topic) {
        super(MSG_CODE);
        this.mid = UUID.randomUUID();
        this.message = message;
        this.topic = topic;
    }

    @Override
    public String toString() {
        return "PSProtocolMessage{" +
                "topic=" + topic + "," +
                "message=" + message +
                '}';
    }
}
