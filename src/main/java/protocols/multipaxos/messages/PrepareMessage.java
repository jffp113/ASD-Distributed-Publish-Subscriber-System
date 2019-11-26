package protocols.multipaxos.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;

import java.io.Serializable;

public class PrepareMessage extends ProtocolMessage implements Serializable {

    public final static short MSG_CODE = 2005;
    public static final ISerializer<PrepareMessage> serializer = new ISerializer<PrepareMessage>() {
        @Override
        public void serialize(PrepareMessage m, ByteBuf out) {
            out.writeInt(m.sequenceNumber);
        }

        @Override
        public PrepareMessage deserialize(ByteBuf in) {
            return new PrepareMessage(in.readInt());
        }

        @Override
        public int serializedSize(PrepareMessage m) {
            return Integer.BYTES;
        }
    };
    private int sequenceNumber;

    public PrepareMessage(int sequenceNumber) {
        super(MSG_CODE);
        this.sequenceNumber = sequenceNumber;
    }

    public int getSequenceNumber() {
        return sequenceNumber;
    }
}
