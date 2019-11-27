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
            out.writeInt(m.replicaSet.size());
            for (Host h : m.replicaSet) {
                h.serialize(out);
            }
        }

        @Override
        public AddReplicaResponseMessage deserialize(ByteBuf in) throws UnknownHostException {
            int replicaSN = in.readInt();
            int instance = in.readInt();
            int leaderSN = in.readInt();
            List<Host> replicaSet = new LinkedList<>();
            int replicaSetSize = in.readInt();
            for (int i = 0; i < replicaSetSize; i++) {
                replicaSet.add(Host.deserialize(in));
            }
            return new AddReplicaResponseMessage(replicaSet, replicaSN, leaderSN, instance);
        }

        @Override
        public int serializedSize(AddReplicaResponseMessage m) {
            return 4 * Integer.BYTES + m.replicaSet.size() * 6;
        }
    };
    private List<Host> replicaSet;
    private int replicaSN;
    private int instance;
    private int leaderSN;

    public AddReplicaResponseMessage(List<Host> replicaSet, int replicaSN, int leaderSN, int instance) {
        super(MSG_CODE);
        this.replicaSet = replicaSet;
        this.replicaSN = replicaSN;
        this.leaderSN = leaderSN;
        this.instance = instance;
    }

    public int getInstance() {
        return instance;
    }

    public int getReplicaSN() {
        return replicaSN;
    }

    public List<Host> getReplicaSet() {
        return replicaSet;
    }

    public int getLeaderSN() {
        return leaderSN;
    }
}
