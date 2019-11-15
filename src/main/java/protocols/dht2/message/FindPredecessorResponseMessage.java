package protocols.dht2.message;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.Host;
import network.ISerializer;

import java.net.UnknownHostException;


public class FindPredecessorResponseMessage extends ProtocolMessage {

    public final static short MSG_CODE = 23482;
    private Host predecessor;

    public FindPredecessorResponseMessage(Host predecessor) {
        super(FindPredecessorResponseMessage.MSG_CODE);
        this.predecessor = predecessor;
    }

    public Host getPredecessor() {
        return predecessor;
    }

    public static final ISerializer<FindPredecessorResponseMessage> serializer = new ISerializer<FindPredecessorResponseMessage>() {
        @Override
        public void serialize(FindPredecessorResponseMessage m, ByteBuf out) {
            m.predecessor.serialize(out);
        }

        @Override
        public FindPredecessorResponseMessage deserialize(ByteBuf in) throws UnknownHostException {
            Host predecessor = Host.deserialize(in);

            return new FindPredecessorResponseMessage(predecessor);
        }

        @Override
        public int serializedSize(FindPredecessorResponseMessage m) {
            return m.predecessor.serializedSize();
        }
    };

}
