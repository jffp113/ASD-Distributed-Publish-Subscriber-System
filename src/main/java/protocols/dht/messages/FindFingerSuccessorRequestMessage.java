package protocols.dht.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.Host;
import network.ISerializer;

import java.net.UnknownHostException;

public class FindFingerSuccessorRequestMessage extends ProtocolMessage {
    public static short MSG_CODE = 10027;

    private int nodeId;
    private Host requesterNode;
    private int next;

    public FindFingerSuccessorRequestMessage(int nodeId, Host host, int next) {
        super(MSG_CODE);
        this.nodeId = nodeId;
        this.requesterNode = host;
        this.next = next;
    }

    public int getNext() {
        return next;
    }

    public int getNodeId() {
        return nodeId;
    }

    public Host getRequesterNode() {
        return requesterNode;
    }

    public static final ISerializer<FindFingerSuccessorRequestMessage> serializer = new ISerializer<FindFingerSuccessorRequestMessage>() {
        @Override
        public void serialize(FindFingerSuccessorRequestMessage m, ByteBuf out) {
            out.writeInt(m.nodeId);
            m.requesterNode.serialize(out);
            out.writeInt(m.next);
        }

        @Override
        public FindFingerSuccessorRequestMessage deserialize(ByteBuf in) throws UnknownHostException {
            return new FindFingerSuccessorRequestMessage(in.readInt(), Host.deserialize(in), in.readInt());
        }

        @Override
        public int serializedSize(FindFingerSuccessorRequestMessage m) {
            return 2 * Integer.BYTES + m.requesterNode.serializedSize();
        }
    };
}
