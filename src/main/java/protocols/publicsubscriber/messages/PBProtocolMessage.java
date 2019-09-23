package protocols.publicsubscriber.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.Host;
import network.ISerializer;
import protocols.partialmembership.messages.GossipProtocolMessage;

import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class PBProtocolMessage extends ProtocolMessage {

    public final static short MSG_CODE = 102;
    private UUID mid;
    private String message;
    private String topic;
    private volatile int size = -1;

    public PBProtocolMessage(String message, String topic) {
        super(MSG_CODE);
        this.mid = UUID.randomUUID();
        this.message = message;
        this.topic = topic;
    }

    @Override
    public String toString() {
        return "PBProtocolMessage{" +
                "topic=" + topic + "," +
                "message=" + message +
                '}';
    }

    public static final ISerializer<PBProtocolMessage> serializer = new ISerializer<PBProtocolMessage>() {
        @Override
        public void serialize(PBProtocolMessage pbProtocolMessage, ByteBuf out) {
            out.writeShort(pbProtocolMessage.topic.length());
            out.writeBytes(pbProtocolMessage.topic.getBytes());
            out.writeShort(pbProtocolMessage.message.length());
            out.writeBytes(pbProtocolMessage.message.getBytes());
        }

        @Override
        public PBProtocolMessage deserialize(ByteBuf in) throws UnknownHostException {
            short topicSize = in.readShort();
            byte[] topic = new byte[topicSize];
            in.readBytes(topic);
            short messageSize = in.readShort();
            byte[] message = new byte[messageSize];
            in.readBytes(message);

            return new PBProtocolMessage(new String(message),new String(topic));
        }

        @Override
        public int serializedSize(PBProtocolMessage pbProtocolMessage) {
            return pbProtocolMessage.size;
        }
    };
}
