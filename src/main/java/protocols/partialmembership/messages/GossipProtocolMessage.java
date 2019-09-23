package protocols.partialmembership.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.Host;
import network.ISerializer;

import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class GossipProtocolMessage extends ProtocolMessage {

    public final static short MSG_CODE = 101;
    private final Set<Host> sample;
    private volatile int size = -1;

    public GossipProtocolMessage(Collection<Host> peers) {
        super(GossipProtocolMessage.MSG_CODE);
        this.sample = new HashSet<>(peers);
    }

    public Set<Host> getSample() {
        return sample;
    }

    @Override
    public String toString() {
        return "GossipMessage{" +
                "payload=" + sample +
                '}';
    }

    public static final ISerializer<GossipProtocolMessage> serializer = new ISerializer<GossipProtocolMessage>() {
        @Override
        public void serialize(GossipProtocolMessage gossipMessage, ByteBuf out) {
            out.writeShort(gossipMessage.sample.size());
            for(Host h: gossipMessage.sample) {
                h.serialize(out);
            }
        }

        @Override
        public GossipProtocolMessage deserialize(ByteBuf in) throws UnknownHostException {
            short size = in.readShort();
            Set<Host> payload = new HashSet<>();
            for(short i = 0; i < size; i++) {
                payload.add(Host.deserialize(in));
            }
            return new GossipProtocolMessage(payload);
        }

        @Override
        public int serializedSize(GossipProtocolMessage gossipMessage) {
            if(gossipMessage.size == -1) {
                gossipMessage.size = 2; //short size
                for (Host h : gossipMessage.sample) {
                    gossipMessage.size += h.serializedSize();
                }
            }
            return gossipMessage.size;
        }
    };
}
