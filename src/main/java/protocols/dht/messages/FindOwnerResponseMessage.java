package protocols.dht.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.Host;
import network.ISerializer;

import java.net.UnknownHostException;

public class FindOwnerResponseMessage extends ProtocolMessage {
    public static short MSG_CODE = 10340;

    private String topic;
    private Host requesterNode;

    public FindOwnerResponseMessage(String topic, Host host) {
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

    public static final ISerializer<FindOwnerResponseMessage> serializer = new ISerializer<FindOwnerResponseMessage>() {
        @Override
        public void serialize(FindOwnerResponseMessage m, ByteBuf out) {
            out.writeInt(m.topic.length());
            out.writeBytes(m.topic.getBytes());
            m.requesterNode.serialize(out);
        }

        @Override
        public FindOwnerResponseMessage deserialize(ByteBuf in) throws UnknownHostException {
            byte[] b = new byte[in.readInt()];
            in.readBytes(b);
            return new FindOwnerResponseMessage(new String(b), Host.deserialize(in));
        }

        @Override
        public int serializedSize(FindOwnerResponseMessage m) {
            return Integer.BYTES*(m.getTopic().length() + 1) + m.requesterNode.serializedSize();
        }
    };
}
