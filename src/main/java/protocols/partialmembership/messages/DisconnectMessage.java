package protocols.partialmembership.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.Host;
import network.ISerializer;

import java.net.UnknownHostException;

public class DisconnectMessage extends ProtocolMessage {
    public final static short MSG_CODE = 1034;
    private final Host node;

    public DisconnectMessage(Host node) {
        super(MSG_CODE);
        this.node = node;
    }

    @Override
    public String toString() {
        return "DisconnectMessage{" +
                "node= " + node.toString() +
                '}';
    }

    public Host getNode() {
        return node;
    }

    public static final ISerializer<DisconnectMessage> serializer = new ISerializer<DisconnectMessage>() {
        @Override
        public void serialize(DisconnectMessage disconnectMessage, ByteBuf out) {
            disconnectMessage.getNode().serialize(out);
        }

        @Override
        public DisconnectMessage deserialize(ByteBuf in) throws UnknownHostException {
            return new DisconnectMessage(Host.deserialize(in));
        }

        @Override
        public int serializedSize(DisconnectMessage disconnectMessage) {
            return disconnectMessage.node.serializedSize();
        }
    };
}
