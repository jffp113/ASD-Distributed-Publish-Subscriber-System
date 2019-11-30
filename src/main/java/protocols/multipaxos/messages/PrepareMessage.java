package protocols.multipaxos.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;

import java.io.Serializable;

public class PrepareMessage extends ProtocolMessage implements Serializable {

    public final static short MSG_CODE = 2005;

    private int sequenceNumber;
    public static final ISerializer<PrepareMessage> serializer = new ISerializer<PrepareMessage>() {
        @Override
        public void serialize(PrepareMessage m, ByteBuf out) {
            out.writeInt(m.sequenceNumber);
            out.writeInt(m.paxosInstance);
        }

        @Override
        public PrepareMessage deserialize(ByteBuf in) {
            return new PrepareMessage(in.readInt(), in.readInt());
        }

        @Override
        public int serializedSize(PrepareMessage m) {
            return 2 * Integer.BYTES;
        }
    };
    private int paxosInstance;

    public int getSequenceNumber() {
        return sequenceNumber;
    }

    public PrepareMessage(int sequenceNumber, int paxosInstance) {
        super(MSG_CODE);
        this.sequenceNumber = sequenceNumber;
        this.paxosInstance = paxosInstance;
    }

    public int getPaxosInstance() {
        return paxosInstance;
    }
}
