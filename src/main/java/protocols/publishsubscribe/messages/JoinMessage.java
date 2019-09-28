package protocols.publishsubscribe.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.Host;
import network.ISerializer;

import java.net.UnknownHostException;
import java.util.UUID;

public class JoinMessage extends ProtocolMessage {
    public final static short MSG_CODE = 103;
    private final Host node;
    private UUID mid;
    private volatile int size = -1;

    public static final ISerializer<JoinMessage> serializer = new ISerializer<JoinMessage>() {
        @Override
        public void serialize(JoinMessage joinMessage, ByteBuf out) {
            joinMessage.getNode().serialize(out);
        }

        @Override
        public JoinMessage deserialize(ByteBuf in) throws UnknownHostException {
            return new JoinMessage(Host.deserialize(in));
        }

        @Override
        public int serializedSize(JoinMessage joinMessage) {
            return joinMessage.getNode().serializedSize();
        }
    };

    public JoinMessage(Host node) {
        super(MSG_CODE);
        this.mid = UUID.randomUUID();
        this.node = node;
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
}
