package protocols.partialmembership.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.Host;
import network.ISerializer;

import java.net.UnknownHostException;
import java.util.UUID;

public class ForwardJoinMessage extends ProtocolMessage {
    public final static short MSG_CODE = 1088;
    private final Host joinerHost;
    private final Host senderHost;
    private int ttl;
    private UUID mid;
    private volatile int size = -1;

    public static final ISerializer<ForwardJoinMessage> serializer = new ISerializer<ForwardJoinMessage>() {
        @Override
        public void serialize(ForwardJoinMessage joinMessage, ByteBuf out) {
            joinMessage.getJoinerHost().serialize(out);
            joinMessage.getSenderHost().serialize(out);
            out.writeInt(joinMessage.getTtl());
        }

        @Override
        public ForwardJoinMessage deserialize(ByteBuf in) throws UnknownHostException {
            Host joinerHost = Host.deserialize(in);
            Host senderHost = Host.deserialize(in);
            int ttl = in.readInt();
            return new ForwardJoinMessage(joinerHost, senderHost, ttl);
        }

        @Override
        public int serializedSize(ForwardJoinMessage joinMessage) {
            return joinMessage.getSenderHost().serializedSize() + joinMessage.getJoinerHost().serializedSize() + Integer.BYTES;
        }
    };

    public ForwardJoinMessage(Host joinerHost, Host senderHost, int ttl) {
        super(MSG_CODE);
        this.mid = UUID.randomUUID();
        this.joinerHost = joinerHost;
        this.senderHost = senderHost;
        this.ttl = ttl;
    }

    @Override
    public String toString() {
        return "PSProtocolMessage{" +
                "joinerHost= " + joinerHost.toString() +
                "senderHost= " + senderHost.toString() +
                "ttl= " + ttl +
                '}';
    }

    public Host getJoinerHost() {
        return joinerHost;
    }

    public Host getSenderHost() {
        return senderHost;
    }

    public int getTtl() {
        return ttl;
    }

    public void setTtl(int ttl) {
        this.ttl = ttl;
    }
}
