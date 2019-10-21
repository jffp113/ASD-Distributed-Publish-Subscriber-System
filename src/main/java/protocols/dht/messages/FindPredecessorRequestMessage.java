package protocols.dht.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;

import java.io.Serializable;

public class FindPredecessorRequestMessage extends ProtocolMessage implements Serializable {

    public final static short MSG_CODE = 24301;

    public FindPredecessorRequestMessage() {
        super(FindPredecessorRequestMessage.MSG_CODE);
    }

    public static final ISerializer<FindPredecessorRequestMessage> serializer = new ISerializer<FindPredecessorRequestMessage>() {
        @Override
        public void serialize(FindPredecessorRequestMessage m, ByteBuf out) {
        }

        @Override
        public FindPredecessorRequestMessage deserialize(ByteBuf in) {
            return new FindPredecessorRequestMessage();
        }

        @Override
        public int serializedSize(FindPredecessorRequestMessage m) {
            return 0;
        }
    };
}
