package protocols.dht.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;

public class FindSuccessorResponseMessage extends ProtocolMessage {
    public static short MSG_CODE = 10001;
    private int nodeId;

    public FindSuccessorResponseMessage(int nodeId) {
        super(MSG_CODE);
        this.nodeId = nodeId;
    }

    public int getNodeId() {
        return nodeId;
    }

    public static final ISerializer<FindSuccessorResponseMessage> serializer = new ISerializer<FindSuccessorResponseMessage>() {
        @Override
        public void serialize(FindSuccessorResponseMessage m, ByteBuf out) {
            out.writeInt(m.getNodeId());
        }

        @Override
        public FindSuccessorResponseMessage deserialize(ByteBuf in) {
            return new FindSuccessorResponseMessage(in.readInt());
        }

        @Override
        public int serializedSize(FindSuccessorResponseMessage m) {
            return Integer.BYTES;
        }
    };
}
