package protocols.multipaxos.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;
import protocols.multipaxos.OrderOperation;

public class ForwardProposeMessage extends ProtocolMessage {
    public final static short MSG_CODE = 69;
    public static final ISerializer<ForwardProposeMessage> serializer = new ISerializer<ForwardProposeMessage>() {
        @Override
        public void serialize(ForwardProposeMessage m, ByteBuf out) {
            m.operation.serialize(out);
        }

        @Override
        public ForwardProposeMessage deserialize(ByteBuf in) {
            return new ForwardProposeMessage(OrderOperation.deserialize(in));
        }

        @Override
        public int serializedSize(ForwardProposeMessage m) {
            return m.operation.serializedSize();
        }
    };
    private OrderOperation operation;

    public ForwardProposeMessage(OrderOperation operation) {
        super(MSG_CODE);
        this.operation = operation;
    }

    public OrderOperation getOperation() {
        return operation;
    }
}
