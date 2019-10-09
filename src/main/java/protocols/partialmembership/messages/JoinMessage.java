package protocols.partialmembership.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.Host;
import network.ISerializer;

import java.net.UnknownHostException;

public class JoinMessage extends ProtocolMessage {
    public final static short MSG_CODE = 103;
    private final Host node;
    private final boolean maxPriority;

    public JoinMessage(Host node) {
        this(node,true);
    }

    public JoinMessage(Host node , boolean maxPriority) {
        super(MSG_CODE);
        this.node = node;
        this.maxPriority = maxPriority;
    }



    @Override
    public String toString() {
        return "PSProtocolMessage{" +
                "node= " + node.toString() +
                '}';
    }

    public Host getNode() {
        return node;
    }

    public boolean isMaxPriority() {
        return maxPriority;
    }

    public static final ISerializer<JoinMessage> serializer = new ISerializer<JoinMessage>() {
        @Override
        public void serialize(JoinMessage joinMessage, ByteBuf out) {
            joinMessage.getNode().serialize(out);
            out.writeBoolean(joinMessage.maxPriority);
        }

        @Override
        public JoinMessage deserialize(ByteBuf in) throws UnknownHostException {
            return new JoinMessage(Host.deserialize(in),in.readBoolean());
        }

        @Override
        public int serializedSize(JoinMessage joinMessage) {
            return joinMessage.node.serializedSize()  + 1;
        }
    };
}
