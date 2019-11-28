package protocols.multipaxos.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;
import protocols.multipaxos.Operation;

import java.net.UnknownHostException;

public class ForwardProposeMessage extends ProtocolMessage {
    public final static short MSG_CODE = 69;

    private Operation operation;

    public ForwardProposeMessage(Operation operation) {
        super(MSG_CODE);
        this.operation = operation;
    }

    public Operation getOperation() {
        return operation;
    }

    public static final ISerializer<ForwardProposeMessage> serializer = new ISerializer<ForwardProposeMessage>() {
        @Override
        public void serialize(ForwardProposeMessage m, ByteBuf out) {
            m.operation.serialize(out);
        }

        @Override
        public ForwardProposeMessage deserialize(ByteBuf in) throws UnknownHostException {
            return new ForwardProposeMessage(Operation.deserialize(in));
        }

        @Override
        public int serializedSize(ForwardProposeMessage m) {
            return m.operation.serializedSize();
        }
    };
}
