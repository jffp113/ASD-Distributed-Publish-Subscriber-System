package protocols.multipaxos;

import io.netty.buffer.ByteBuf;
import network.Host;

import java.net.UnknownHostException;

public class MembershipUpdateContent implements Content {
    private Host replica;

    public MembershipUpdateContent(Host replica) {
        this.replica = replica;
    }

    public Host getReplica() {
        return replica;
    }

    public void serialize(ByteBuf out) {
        replica.serialize(out);
    }

    public static Content deserialize(ByteBuf in) throws UnknownHostException {
        return new MembershipUpdateContent(Host.deserialize(in));
    }

    public int serializedSize() {
        return 6;
    }
}
