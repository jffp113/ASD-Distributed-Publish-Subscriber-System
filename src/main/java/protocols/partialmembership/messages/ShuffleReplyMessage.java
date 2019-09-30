package protocols.partialmembership.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.Host;
import network.ISerializer;

import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class ShuffleReplyMessage extends ProtocolMessage {
    public final static short MSG_CODE = 1002;
    public static final ISerializer<ShuffleReplyMessage> serializer = new ISerializer<ShuffleReplyMessage>() {
        @Override
        public void serialize(ShuffleReplyMessage shuffleReplyMessage, ByteBuf out) {
            out.writeLong(shuffleReplyMessage.mid.getMostSignificantBits());
            out.writeLong(shuffleReplyMessage.mid.getLeastSignificantBits());
            out.writeShort(shuffleReplyMessage.nodes.size());

            for (Host h : shuffleReplyMessage.nodes) {
                h.serialize(out);
            }
        }

        @Override
        public ShuffleReplyMessage deserialize(ByteBuf in) throws UnknownHostException {
            UUID mid = new UUID(in.readLong(), in.readLong());
            short nodesSize = in.readShort();

            Set<Host> nodes = new HashSet<>();

            for (int i = 0; i < nodesSize; i++) {
                nodes.add(Host.deserialize(in));
            }

            return new ShuffleReplyMessage(mid, nodes);
        }

        @Override
        public int serializedSize(ShuffleReplyMessage shuffleReplyMessage) {
            int hostSize = 6;
            return 2*Long.BYTES + Short.BYTES + shuffleReplyMessage.nodes.size() * hostSize;
        }
    };
    private UUID mid;
    private Set<Host> nodes;

    public ShuffleReplyMessage(UUID mid, Set<Host> nodes) {
        super(ShuffleReplyMessage.MSG_CODE);

        this.nodes = nodes;
        this.mid = mid;
    }

    @Override
    public String toString() {
        return "ShuffleReplyMessage{" +
                "mid=" + mid +
                "nodes=" + nodes +
                '}';
    }

    public UUID getMid() {
        return mid;
    }

    public Set<Host> getNodes() {
        return nodes;
    }
}
