package protocols.multipaxos.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;

import java.io.Serializable;
import java.net.UnknownHostException;

public class PrepareOk extends ProtocolMessage implements Serializable {

    public final static short MSG_CODE = 25;

    public PrepareOk(int sequenceNumber) {
        super(MSG_CODE);
        this.sequenceNumber = sequenceNumber;
    }

    public static final ISerializer<PrepareOk> serializer = new ISerializer<PrepareOk>() {
        @Override
        public void serialize(PrepareOk m, ByteBuf out) {
            out.writeInt(m.sequenceNumber);
        }

        @Override
        public PrepareOk deserialize(ByteBuf in) throws UnknownHostException {
            return new PrepareOk(in.readInt());
        }

        @Override
        public int serializedSize(PrepareOk m) {
            return Integer.BYTES;
        }
    };

    private int sequenceNumber;

    public int getSequenceNumber() {
        return sequenceNumber;
    }

}
