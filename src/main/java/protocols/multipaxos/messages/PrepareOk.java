package protocols.multipaxos.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;
import protocols.multipaxos.Operation;

import java.io.Serializable;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;

public class PrepareOk extends ProtocolMessage implements Serializable {

    public final static short MSG_CODE = 25;
    public static final ISerializer<PrepareOk> serializer = new ISerializer<PrepareOk>() {
        @Override
        public void serialize(PrepareOk m, ByteBuf out) {
            out.writeInt(m.sequenceNumber);
            out.writeInt(m.toAccept.size());
            for (Operation op : m.toAccept) {
                op.serialize(out);
            }
        }

        @Override
        public PrepareOk deserialize(ByteBuf in) throws UnknownHostException {
            int sequenceNumber = in.readInt();
            int size = in.readInt();
            Set<Operation> toAccept = new HashSet<>();
            for (int i = 0; i < size; i++) {
                toAccept.add(Operation.deserialize(in));
            }

            return new PrepareOk(sequenceNumber, toAccept);
        }

        @Override
        public int serializedSize(PrepareOk m) {
            int size = 2 * Integer.BYTES;
            for (Operation op : m.toAccept) {
                size += op.serializedSize();
            }
            return size;
        }
    };
    private int sequenceNumber;
    private Set<Operation> toAccept;

    public PrepareOk(int sequenceNumber, Set<Operation> toAccept) {
        super(MSG_CODE);
        this.sequenceNumber = sequenceNumber;
        this.toAccept = toAccept;
    }

    public int getSequenceNumber() {
        return sequenceNumber;
    }

    public Set<Operation> getToAccept() {
        return toAccept;
    }

}
