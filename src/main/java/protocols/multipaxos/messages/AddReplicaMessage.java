package protocols.multipaxos.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.Host;
import network.ISerializer;

import java.io.Serializable;
import java.net.UnknownHostException;

public class AddReplicaMessage extends ProtocolMessage implements Serializable {

    public final static short MSG_CODE = 20006;
    public static final ISerializer<AddReplicaMessage> serializer = new ISerializer<AddReplicaMessage>() {
        @Override
        public void serialize(AddReplicaMessage m, ByteBuf out) {
            m.replica.serialize(out);
        }

        @Override
        public AddReplicaMessage deserialize(ByteBuf in) throws UnknownHostException {
            return new AddReplicaMessage(Host.deserialize(in));
        }

        @Override
        public int serializedSize(AddReplicaMessage m) {
            return m.replica.serializedSize();
        }
    };
    private Host replica;

    public AddReplicaMessage(Host replica) {
        super(MSG_CODE);
        this.replica = replica;
    }

    public Host getReplica() {
        return replica;
    }
}
