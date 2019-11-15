package protocols.dht2.message;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.Host;
import network.ISerializer;

import java.net.UnknownHostException;

public class FindFingerSuccessorResponseMessage extends ProtocolMessage {
    public static short MSG_CODE = 10091;

    private Host successor;
    private int next;

    public FindFingerSuccessorResponseMessage(Host successor, int next) {
        super(MSG_CODE);
        this.successor = successor;
        this.next = next;
    }

    public Host getSuccessor() {
        return successor;
    }

    public int getNext() {
        return next;
    }

    public static final ISerializer<FindFingerSuccessorResponseMessage> serializer = new ISerializer<FindFingerSuccessorResponseMessage>() {
        @Override
        public void serialize(FindFingerSuccessorResponseMessage m, ByteBuf out) {
            m.successor.serialize(out);
            out.writeInt(m.next);
        }

        @Override
        public FindFingerSuccessorResponseMessage deserialize(ByteBuf in) throws UnknownHostException {
            Host successor = Host.deserialize(in);
            return new FindFingerSuccessorResponseMessage(successor, in.readInt());
        }

        @Override
        public int serializedSize(FindFingerSuccessorResponseMessage m) {
            return m.successor.serializedSize() + Integer.BYTES;
        }
    };
}
