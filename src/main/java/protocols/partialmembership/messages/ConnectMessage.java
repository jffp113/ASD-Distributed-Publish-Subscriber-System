package protocols.partialmembership.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.Host;
import network.ISerializer;

import java.net.UnknownHostException;

public class ConnectMessage extends ProtocolMessage {
    public final static short MSG_CODE = 1035;
    private final Host node;

    public ConnectMessage(Host node) {
        super(MSG_CODE);
        this.node = node;
    }

    @Override
    public String toString() {
        return "ConnectlMessage{" +
                "node= " + node.toString() +
                '}';
    }

    public Host getNode() {
        return node;
    }

    public static final ISerializer<ConnectMessage> serializer = new ISerializer<ConnectMessage>() {
        @Override
        public void serialize(ConnectMessage connectMessage, ByteBuf out) {
            connectMessage.getNode().serialize(out);
        }

        @Override
        public ConnectMessage deserialize(ByteBuf in) throws UnknownHostException {
            return new ConnectMessage(Host.deserialize(in));
        }

        @Override
        public int serializedSize(ConnectMessage connectMessage) {
            return connectMessage.node.serializedSize();
        }
    };
}
