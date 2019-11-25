package protocols.multipaxos;

import io.netty.buffer.ByteBuf;
import persistence.PersistentWritable;

public class Operation implements PersistentWritable {


    private int seq ;

    public static Operation deserialize(ByteBuf in) {
        return new Operation();
    }

    public void serialize(ByteBuf out) {
    }

    public int serializedSize() {
        return 0;
    }

    public void setSeq(int seq) {
        this.seq = seq;
    }

    @Override
    public String serializeToString() {
        return null;
    }

    @Override
    public PersistentWritable deserialize(String object) {
        return null;
    }
}
