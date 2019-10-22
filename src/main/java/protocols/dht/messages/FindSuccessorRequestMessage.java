package protocols.dht.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.Host;
import network.ISerializer;

import java.net.UnknownHostException;

public class FindSuccessorRequestMessage extends ProtocolMessage {
    public static short MSG_CODE = 10000;

    private int nodeId;
    private Host requesterNode;

    public FindSuccessorRequestMessage(int nodeId, Host host) {
        super(MSG_CODE);
        this.nodeId = nodeId;
        this.requesterNode = host;
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
        }

        @Override
        public FindSuccessorRequestMessage deserialize(ByteBuf in) throws UnknownHostException {
            return new FindSuccessorRequestMessage(in.readInt(), Host.deserialize(in));
        }

        @Override
        public int serializedSize(FindSuccessorRequestMessage m) {
            return Integer.BYTES + m.requesterNode.serializedSize();
        }
    };
}
