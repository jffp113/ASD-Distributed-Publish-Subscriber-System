package protocols.dht.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.Host;
import network.ISerializer;

import java.net.UnknownHostException;

public class FindOwnerRequestMessage extends ProtocolMessage {
    public static short MSG_CODE = 10340;

    private String topic;
    private Host requesterNode;

    public FindOwnerRequestMessage(String topic, Host host) {
        super(MSG_CODE);
        this.topic = topic;
        this.requesterNode = host;
    }

    public String getTopic() {
        return topic;
    }

    public Host getRequesterNode() {
        return requesterNode;
    }

    public static final ISerializer<FindOwnerRequestMessage> serializer = new ISerializer<FindOwnerRequestMessage>() {
        @Override
        public void serialize(FindOwnerRequestMessage m, ByteBuf out) {
            out.writeInt(m.topic.length());
            out.writeBytes(m.topic.getBytes());
            m.requesterNode.serialize(out);
        }

        @Override
        public FindOwnerRequestMessage deserialize(ByteBuf in) throws UnknownHostException {
            byte[] b = new byte[in.readInt()];
            in.readBytes(b);
            return new FindOwnerRequestMessage(new String(b), Host.deserialize(in));
        }

        @Override
        public int serializedSize(FindOwnerRequestMessage m) {
            return Integer.BYTES + m.getTopic().length() + m.requesterNode.serializedSize();
        }
    };
}
