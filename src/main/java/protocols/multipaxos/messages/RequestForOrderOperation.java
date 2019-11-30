package protocols.multipaxos.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;
import protocols.multipaxos.Operation;

import java.net.UnknownHostException;

public class RequestForOrderOperation extends ProtocolMessage {
    public final static short MSG_CODE = 20005;

    public static final ISerializer<RequestForOrderOperation> serializer = new ISerializer<RequestForOrderOperation>() {
        @Override
        public void serialize(RequestForOrderOperation m, ByteBuf out) {
            m.operation.serialize(out);
        }

        @Override
        public RequestForOrderOperation deserialize(ByteBuf in) throws UnknownHostException {
            return new RequestForOrderOperation(Operation.deserialize(in));
        }

        @Override
        public int serializedSize(RequestForOrderOperation m) {
            return m.operation.serializedSize();
        }
    };
    private Operation operation;

    public RequestForOrderOperation(Operation operation) {
        super(MSG_CODE);
        this.operation = operation;
    }

    public Operation getOperation() {
        return operation;
    }

}
