package protocols.publishsubscribe.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;
import protocols.multipaxos.Operation;

import java.io.Serializable;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

public class LatePaxosInstanceReplyMessage extends ProtocolMessage implements Serializable {
    public final static short MSG_CODE = 12345;

    private List<Operation> operations;

    public LatePaxosInstanceReplyMessage(List<Operation> operations) {
        super(MSG_CODE);
        this.operations = operations;
    }

    public static final ISerializer<LatePaxosInstanceReplyMessage> serializer = new ISerializer<LatePaxosInstanceReplyMessage>() {
        @Override
        public void serialize(LatePaxosInstanceReplyMessage m, ByteBuf out) {
            out.writeInt(m.operations.size());
            for(Operation operation : m.operations)
                operation.serialize(out);
        }

        @Override
        public LatePaxosInstanceReplyMessage deserialize(ByteBuf in) throws UnknownHostException {
            int size = in.readInt();
            List<Operation> operations = new ArrayList<>(size);

            for(int i = 0 ; i < size; i++){
                operations.add(i,Operation.deserialize(in));
            }

            return new LatePaxosInstanceReplyMessage(operations);
        }

        @Override
        public int serializedSize(LatePaxosInstanceReplyMessage m) {
            return m.operations.get(0).serializedSize() + Integer.BYTES;
        }
    };

    public List<Operation> getOperations() {
        return operations;
    }
}
