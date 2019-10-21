package protocols.dht.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.Host;
import network.ISerializer;

import java.net.UnknownHostException;

public class FindSuccessorRequestMessage extends ProtocolMessage {
    public static short MSG_CODE = 10000;
    // is next field is not being used.
    private static final int NOT_USED = -1;

    private int nodeId;
    private Host requesterNode;
    private int next;

    public FindSuccessorRequestMessage(int nodeId, Host host, int next) {
        super(MSG_CODE);
        this.nodeId = nodeId;
        this.requesterNode = host;
        this.next = next;
    }

    public FindSuccessorRequestMessage(int nodeId, Host host) {
        this(nodeId, host, NOT_USED);
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

    public static final ISerializer<FindSuccessorRequestMessage> serializer = new ISerializer<FindSuccessorRequestMessage>() {
        @Override
        public void serialize(FindSuccessorRequestMessage m, ByteBuf out) {
            out.writeInt(m.nodeId);
            m.requesterNode.serialize(out);
            out.writeInt(m.next);
        }

        @Override
        public FindSuccessorRequestMessage deserialize(ByteBuf in) throws UnknownHostException {
            return new FindSuccessorRequestMessage(in.readInt(), Host.deserialize(in), in.readInt());
        }

        @Override
        public int serializedSize(FindSuccessorRequestMessage m) {
            return 2 * Integer.BYTES + m.requesterNode.serializedSize();
        }
    };
}
