package protocols.multipaxos.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;
import protocols.multipaxos.Operation;

import java.io.Serializable;
import java.net.UnknownHostException;

public class AcceptOperationMessage extends ProtocolMessage implements Serializable {
    public final static short MSG_CODE = 26;

    private Operation operation;

    public AcceptOperationMessage(Operation operation) {
        super(MSG_CODE);
        this.operation = operation;
    }

    public static final ISerializer<AcceptOperationMessage> serializer = new ISerializer<AcceptOperationMessage>() {
        @Override
        public void serialize(AcceptOperationMessage m, ByteBuf out) {
            m.operation.serialize(out);
        }

        @Override
        public AcceptOperationMessage deserialize(ByteBuf in) throws UnknownHostException {
            return new AcceptOperationMessage(Operation.deserialize(in));
        }

        @Override
        public int serializedSize(AcceptOperationMessage m) {
            return m.operation.serializedSize();
        }
    };

    public Operation getOperation() {
        return operation;
    }
}
