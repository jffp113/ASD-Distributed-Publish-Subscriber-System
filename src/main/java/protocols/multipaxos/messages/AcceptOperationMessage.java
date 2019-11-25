package protocols.multipaxos.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;
import protocols.multipaxos.Operation;

import java.io.Serializable;

public class AcceptOperationMessage extends ProtocolMessage implements Serializable {

    public final static short MSG_CODE = 25;

    public static final ISerializer<AcceptOperationMessage> serializer = new ISerializer<AcceptOperationMessage>() {
        @Override
        public void serialize(AcceptOperationMessage m, ByteBuf out) {
            out.writeInt(m.instance);
            m.operation.serialize(out);
        }

        @Override
        public AcceptOperationMessage deserialize(ByteBuf in) {
            return new AcceptOperationMessage(in.readInt(), Operation.deserialize(in));
        }

        @Override
        public int serializedSize(AcceptOperationMessage m) {
            return Integer.BYTES + m.operation.serializedSize();
        }
    };
    private Operation operation;
    private int instance;

    public AcceptOperationMessage(int instance, Operation operation) {
        super(MSG_CODE);
        this.instance = instance;
        this.operation = operation;
    }

    public Operation getOperation() {
        return operation;
    }

    public int getInstance() {
        return instance;
    }
}
