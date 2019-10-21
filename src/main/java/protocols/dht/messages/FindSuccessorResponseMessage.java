package protocols.dht.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.Host;
import network.ISerializer;

import java.net.UnknownHostException;

public class FindSuccessorResponseMessage extends ProtocolMessage {
    public static short MSG_CODE = 10001;

    private Host successor;

    public FindSuccessorResponseMessage(Host successor) {
        super(MSG_CODE);
        this.successor = successor;
    }

    public Host getSuccessor() {
        return successor;
    }

    public static final ISerializer<FindSuccessorResponseMessage> serializer = new ISerializer<FindSuccessorResponseMessage>() {
        @Override
        public void serialize(FindSuccessorResponseMessage m, ByteBuf out) {
            m.successor.serialize(out);
        }

        @Override
        public FindSuccessorResponseMessage deserialize(ByteBuf in) throws UnknownHostException {
            Host successor = Host.deserialize(in);
            return new FindSuccessorResponseMessage(successor);
        }

        @Override
        public int serializedSize(FindSuccessorResponseMessage m) {
            return m.successor.serializedSize();
        }
    };
}
