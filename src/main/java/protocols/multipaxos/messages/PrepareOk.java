package protocols.multipaxos.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.Host;
import network.ISerializer;
import protocols.multipaxos.Operation;

import java.io.Serializable;

public class PrepareOk extends ProtocolMessage implements Serializable {

    public final static short MSG_CODE = 25;

    public PrepareOk() {
        super(MSG_CODE);
    }

    public static final ISerializer<PrepareOk> serializer = new ISerializer<PrepareOk>() {
        @Override
        public void serialize(PrepareOk m, ByteBuf out) {
            out.writeInt(m.sequenceNumber);
            m.operation.serialize(out);
        }

        @Override
        public PrepareOk deserialize(ByteBuf in) {
            return new PrepareOk(in.readInt(), Operation.deserialize(in));
        }

        @Override
        public int serializedSize(PrepareOk m) {
            return Integer.BYTES + m.operation.serializedSize();
        }
    };
    private int sequenceNumber;
    private Operation operation;

    public PrepareOk(int sequenceNumber, Operation operation) {
        super(MSG_CODE);
        this.sequenceNumber = sequenceNumber;
        this.operation = operation;
    }

    public int getSequenceNumber() {
        return sequenceNumber;
    }

    public Operation getOperation() {
        return operation;
    }
}
