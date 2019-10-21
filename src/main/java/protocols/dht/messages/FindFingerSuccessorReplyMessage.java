package protocols.dht.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.Host;
import network.ISerializer;

import java.net.UnknownHostException;

public class FindFingerSuccessorReplyMessage extends ProtocolMessage {
    public static short MSG_CODE = 10091;

    private Host successor;
    private int next;

    public FindFingerSuccessorReplyMessage(Host successor, int next) {
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

    public static final ISerializer<FindFingerSuccessorReplyMessage> serializer = new ISerializer<FindFingerSuccessorReplyMessage>() {
        @Override
        public void serialize(FindFingerSuccessorReplyMessage m, ByteBuf out) {
            m.successor.serialize(out);
            out.writeInt(m.next);
        }

        @Override
        public FindFingerSuccessorReplyMessage deserialize(ByteBuf in) throws UnknownHostException {
            Host successor = Host.deserialize(in);
            return new FindFingerSuccessorReplyMessage(successor, in.readInt());
        }

        @Override
        public int serializedSize(FindFingerSuccessorReplyMessage m) {
            return m.successor.serializedSize() + Integer.BYTES;
        }
    };
}
