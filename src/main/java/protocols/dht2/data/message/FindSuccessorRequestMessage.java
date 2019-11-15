package protocols.dht2.data.message;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.Host;
import network.ISerializer;
import protocols.dht2.data.ID;

import java.net.UnknownHostException;

public class FindSuccessorRequestMessage extends ProtocolMessage {
    public static short MSG_CODE = 10000;

    private ID nodeId;
    private Host requesterNode;

    public FindSuccessorRequestMessage(ID nodeId, Host host) {
        super(MSG_CODE);
        this.nodeId = nodeId;
        this.requesterNode = host;
    }

    public ID getNodeId() {
        return nodeId;
    }

    public Host getRequesterNode() {
        return requesterNode;
    }

    public static final ISerializer<FindSuccessorRequestMessage> serializer = new ISerializer<FindSuccessorRequestMessage>() {
        @Override
        public void serialize(FindSuccessorRequestMessage m, ByteBuf out) {
            m.nodeId.serialize(out);
            m.requesterNode.serialize(out);
        }

        @Override
        public FindSuccessorRequestMessage deserialize(ByteBuf in) throws UnknownHostException {
            return new FindSuccessorRequestMessage(ID.deserialize(in), Host.deserialize(in));
        }

        @Override
        public int serializedSize(FindSuccessorRequestMessage m) {
            return ID.serializeSize() + m.requesterNode.serializedSize();
        }
    };
}
