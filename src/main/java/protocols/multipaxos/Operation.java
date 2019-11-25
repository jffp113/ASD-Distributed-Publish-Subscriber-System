package protocols.multipaxos;

import io.netty.buffer.ByteBuf;

public class Operation {

    public static Operation deserialize(ByteBuf in) {
        return new Operation();
    }

    public void serialize(ByteBuf out) {
    }

    public int serializedSize() {
        return 0;
    }
}
