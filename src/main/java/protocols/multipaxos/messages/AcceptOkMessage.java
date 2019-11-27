package protocols.multipaxos.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;
import protocols.multipaxos.Operation;

import java.io.Serializable;
import java.net.UnknownHostException;

public class AcceptOkMessage extends ProtocolMessage implements Serializable {
    public final static short MSG_CODE = 70;

    private Operation operation;

    public AcceptOkMessage(Operation operation) {
        super(MSG_CODE);
        this.operation = operation;
    }

    public Operation getOperation() {
        return operation;
    }

    public static final ISerializer<AcceptOkMessage> serializer = new ISerializer<AcceptOkMessage>() {
        @Override
        public void serialize(AcceptOkMessage m, ByteBuf out) {
            m.operation.serialize(out);
        }

        @Override
        public AcceptOkMessage deserialize(ByteBuf in) throws UnknownHostException {
            return new AcceptOkMessage(Operation.deserialize(in));
        }

        @Override
        public int serializedSize(AcceptOkMessage m) {
            return m.operation.serializedSize();
        }
    };

}
