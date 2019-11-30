package protocols.multipaxos.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.Host;
import network.ISerializer;

import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;

public class AddReplicaResponseMessage extends ProtocolMessage {
    public final static short MSG_CODE = 20007;

    public static ISerializer<AddReplicaResponseMessage> serializer = new ISerializer<AddReplicaResponseMessage>() {
        @Override
        public void serialize(AddReplicaResponseMessage m, ByteBuf out) {
            out.writeInt(m.replicaSN);
            out.writeInt(m.instance);
            out.writeInt(m.leaderSN);
            out.writeInt(m.replicas.size());
            for (Host h : m.replicas) {
                h.serialize(out);
            }
        }

        @Override
        public AddReplicaResponseMessage deserialize(ByteBuf in) throws UnknownHostException {
            int replicaSN = in.readInt();
            int instance = in.readInt();
            int leaderSN = in.readInt();
            List<Host> replicas = new LinkedList<>();
            int replicaSize = in.readInt();
            for (int i = 0; i < replicaSize; i++) {
                replicas.add(Host.deserialize(in));
            }
            return new AddReplicaResponseMessage(replicas, replicaSN, leaderSN, instance);
        }

        @Override
        public int serializedSize(AddReplicaResponseMessage m) {
            return 4 * Integer.BYTES + m.replicas.size() * 6;
        }
    };
    private int replicaSN;
    private int instance;
    private int leaderSN;
    private List<Host> replicas;

    public int getInstance() {
        return instance;
    }

    public int getReplicaSN() {
        return replicaSN;
    }

    public AddReplicaResponseMessage(List<Host> replicas, int replicaSN, int leaderSN, int instance) {
        super(MSG_CODE);
        this.replicas = replicas;
        this.replicaSN = replicaSN;
        this.leaderSN = leaderSN;
        this.instance = instance;
    }

    public int getLeaderSN() {
        return leaderSN;
    }

    public List<Host> getReplicas() {
        return replicas;
    }
}
