package protocols.multipaxos.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.Host;
import network.ISerializer;

import java.io.Serializable;
import java.net.UnknownHostException;

public class AddReplicaMessage extends ProtocolMessage implements Serializable {

    public final static short MSG_CODE = 20005;
    public static final ISerializer<AddReplicaMessage> serializer = new ISerializer<AddReplicaMessage>() {
        @Override
        public void serialize(AddReplicaMessage m, ByteBuf out) {
            m.requester.serialize(out);
        }

        @Override
        public AddReplicaMessage deserialize(ByteBuf in) throws UnknownHostException {
            return new AddReplicaMessage(Host.deserialize(in));
        }

        @Override
        public int serializedSize(AddReplicaMessage m) {
            return m.requester.serializedSize();
        }
    };
    private Host requester;

    public AddReplicaMessage(Host requester) {
        super(MSG_CODE);
        this.requester = requester;
    }

    public Host getRequester() {
        return requester;
    }

    public void setRequester(Host requester) {
        this.requester = requester;
    }
}
