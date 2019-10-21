package protocols.dht.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.Host;
import network.ISerializer;

import java.net.UnknownHostException;


public class FindPredecessorReplyMessage extends ProtocolMessage {

    public final static short MSG_CODE = 23482;
    private Host predecessor;

    public FindPredecessorReplyMessage(Host predecessor) {
        super(FindPredecessorReplyMessage.MSG_CODE);
        this.predecessor = predecessor;
    }

    public Host getPredecessor() {
        return predecessor;
    }

    public static final ISerializer<FindPredecessorReplyMessage> serializer = new ISerializer<FindPredecessorReplyMessage>() {
        @Override
        public void serialize(FindPredecessorReplyMessage m, ByteBuf out) {
            m.predecessor.serialize(out);
        }

        @Override
        public FindPredecessorReplyMessage deserialize(ByteBuf in) throws UnknownHostException {
            Host predecessor = Host.deserialize(in);

            return new FindPredecessorReplyMessage(predecessor);
        }

        @Override
        public int serializedSize(FindPredecessorReplyMessage m) {
            return m.predecessor.serializedSize();
        }
    };

}
