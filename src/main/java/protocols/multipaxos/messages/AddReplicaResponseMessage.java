package protocols.multipaxos.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.Host;
import network.ISerializer;

import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;

public class AddReplicaResponseMessage extends ProtocolMessage {
    public final static short MSG_CODE = 20007;

    private Set<Host> replicaSet;
    private int seqNumber;
    private int instance;

    public AddReplicaResponseMessage(Set<Host> replicaSet, int seqNumber, int instance) {
        super(MSG_CODE);
        this.replicaSet = replicaSet;
        this.seqNumber = seqNumber;
        this.instance = instance;
    }

    public int getSeqNumber() {
        return seqNumber;
    }

    public int getInstance() {
        return instance;
    }

    public Set<Host> getReplicaSet() {
        return replicaSet;
    }

    public static ISerializer<AddReplicaResponseMessage> serializer = new ISerializer<AddReplicaResponseMessage>() {
        @Override
        public void serialize(AddReplicaResponseMessage m, ByteBuf out) {
            out.writeInt(m.seqNumber);
            out.writeInt(m.instance);
            out.writeInt(m.replicaSet.size());
            for (Host h : m.replicaSet) {
                h.serialize(out);
            }
        }

        @Override
        public AddReplicaResponseMessage deserialize(ByteBuf in) throws UnknownHostException {
            int seqNumber = in.readInt();
            int instance = in.readInt();
            Set<Host> replicaSet = new HashSet<>();
            int replicaSetSize = in.readInt();
            for (int i = 0; i < replicaSetSize; i++) {
                replicaSet.add(Host.deserialize(in));
            }
            return new AddReplicaResponseMessage(replicaSet, seqNumber, instance);
        }

        @Override
        public int serializedSize(AddReplicaResponseMessage m) {
            return 3 * Integer.BYTES + m.replicaSet.size() * 6;
        }
    };
}
