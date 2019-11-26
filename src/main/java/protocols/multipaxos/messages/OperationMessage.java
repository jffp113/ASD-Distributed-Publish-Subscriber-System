package protocols.multipaxos.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;
import protocols.multipaxos.OrderOperation;

import java.io.Serializable;

public class OperationMessage extends ProtocolMessage implements Serializable {

    public final static short MSG_CODE = 235;

    public static final ISerializer<OperationMessage> serializer = new ISerializer<OperationMessage>() {
        @Override
        public void serialize(OperationMessage m, ByteBuf out) {
            m.operation.serialize(out);
        }

        @Override
        public OperationMessage deserialize(ByteBuf in) {
            return new OperationMessage(OrderOperation.deserialize(in));
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
    private OrderOperation operation;

    public OperationMessage(OrderOperation operation) {
        super(MSG_CODE);
        this.operation = operation;
    }

    public OrderOperation getOperation() {
        return operation;
    }
}
