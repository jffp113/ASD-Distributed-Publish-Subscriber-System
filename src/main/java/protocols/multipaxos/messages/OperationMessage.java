package protocols.multipaxos.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;
import protocols.multipaxos.Operation;

import java.io.Serializable;
import java.net.UnknownHostException;

public class OperationMessage extends ProtocolMessage implements Serializable {

    public final static short MSG_CODE = 235;

    public static final ISerializer<OperationMessage> serializer = new ISerializer<OperationMessage>() {
        @Override
        public void serialize(OperationMessage m, ByteBuf out) {
            m.operation.serialize(out);
        }

        @Override
        public OperationMessage deserialize(ByteBuf in) throws UnknownHostException {
            return new OperationMessage(Operation.deserialize(in));
        }

        @Override
        public int serializedSize(OperationMessage m) {
            return m.operation.serializedSize();
        }
    };
    private int sequenceNumber;

    public OperationMessage() {
        super(MSG_CODE);
    }
    private Operation operation;

    public OperationMessage(Operation operation) {
        super(MSG_CODE);
        this.operation = operation;
    }

    public Operation getOperation() {
        return operation;
    }
}
