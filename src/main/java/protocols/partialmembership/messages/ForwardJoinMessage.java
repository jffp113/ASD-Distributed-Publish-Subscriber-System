package protocols.partialmembership.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.Host;
import network.ISerializer;

import java.net.UnknownHostException;

public class ForwardJoinMessage extends ProtocolMessage {
    public final static short MSG_CODE = 1088;
    private final Host newNode;
    private int ttl;
    private final Host sender;

    public static final ISerializer<ForwardJoinMessage> serializer = new ISerializer<ForwardJoinMessage>() {
        @Override
        public void serialize(ForwardJoinMessage joinMessage, ByteBuf out) {
            joinMessage.getNewNode().serialize(out);
            joinMessage.getSender().serialize(out);
            out.writeInt(joinMessage.getTtl());
        }

        @Override
        public ForwardJoinMessage deserialize(ByteBuf in) throws UnknownHostException {
            Host newNode = Host.deserialize(in);
            Host sender = Host.deserialize(in);
            int ttl = in.readInt();
            return new ForwardJoinMessage(newNode, sender, ttl);
        }

        @Override
        public int serializedSize(ForwardJoinMessage joinMessage) {
            return joinMessage.sender.serializedSize() + joinMessage.newNode.serializedSize() + Integer.BYTES;
        }
    };

    public ForwardJoinMessage(Host newNode, Host sender, int ttl) {
        super(MSG_CODE);
        this.newNode = newNode;
        this.sender = sender;
        this.ttl = ttl;
    }

    public Host getNewNode() {
        return newNode;
    }

    public Host getSender() {
        return sender;
    }

    public int getTtl() {
        return ttl;
    }

    @Override
    public String toString() {
        return "PSProtocolMessage{" +
                "newNode= " + newNode.toString() +
                "sender= " + sender.toString() +
                "ttl= " + ttl +
                '}';
    }
}
