package protocols.multipaxos.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;
import protocols.multipaxos.OrderOperation;

import java.io.Serializable;

public class AcceptOkMessage extends ProtocolMessage implements Serializable {

    public final static short MSG_CODE = 70;

    public static final ISerializer<AcceptOkMessage> serializer = new ISerializer<AcceptOkMessage>() {
        @Override
        public void serialize(AcceptOkMessage m, ByteBuf out) {
            out.writeInt(m.instance);
            m.operation.serialize(out);
        }

        @Override
        public AcceptOkMessage deserialize(ByteBuf in) {
            return new AcceptOkMessage(in.readInt(), OrderOperation.deserialize(in));
        }

        @Override
        public int serializedSize(AcceptOkMessage m) {
            return Integer.BYTES + m.operation.serializedSize();
        }
    };
    private int instance;
    private OrderOperation operation;

    public AcceptOkMessage(int instance, OrderOperation operation) {
        super(MSG_CODE);
        this.instance = instance;
        this.operation = operation;
    }

    public int getInstance() {
        return instance;
    }

    public OrderOperation getOperation() {
        return operation;
    }
}
