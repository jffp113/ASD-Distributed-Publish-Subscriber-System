package protocols.partialmembership.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.Host;
import network.ISerializer;

import java.net.UnknownHostException;

public class RejectMessage extends ProtocolMessage {
    public final static short MSG_CODE = 1434;
    private final Host node;

    public RejectMessage(Host node) {
        super(MSG_CODE);
        this.node = node;
    }

    @Override
    public String toString() {
        return "RejectMessage{" +
                "node= " + node.toString() +
                '}';
    }

    public Host getNode() {
        return node;
    }

    public static final ISerializer<RejectMessage> serializer = new ISerializer<RejectMessage>() {
        @Override
        public void serialize(RejectMessage rejectMessage, ByteBuf out) {
            rejectMessage.getNode().serialize(out);
        }

        @Override
        public RejectMessage deserialize(ByteBuf in) throws UnknownHostException {
            return new RejectMessage(Host.deserialize(in));
        }

        @Override
        public int serializedSize(RejectMessage rejectMessage) {
            return rejectMessage.node.serializedSize();
        }
    };
}
