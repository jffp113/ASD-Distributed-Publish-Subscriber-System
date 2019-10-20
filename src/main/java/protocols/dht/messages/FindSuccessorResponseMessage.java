package protocols.dht.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.Host;
import network.ISerializer;
import protocols.dht.FingerEntry;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

public class FindSuccessorResponseMessage extends ProtocolMessage {


    public static short MSG_CODE = 10001;
    private int nodeId;
    private List<FingerEntry> fingers;
    private Host predecessor;


    public FindSuccessorResponseMessage(int nodeId, List<FingerEntry> fingers,Host predecessor) {
        super(MSG_CODE);
        this.nodeId = nodeId;
        this.fingers = fingers;
        this.predecessor = predecessor;
    }

    public int getNodeId() {
        return nodeId;
    }


    public List<FingerEntry> getFingers() {
        return fingers;
    }

    public Host getPredecessor() {
        return predecessor;
    }

    public static final ISerializer<FindSuccessorResponseMessage> serializer = new ISerializer<FindSuccessorResponseMessage>() {
        @Override
        public void serialize(FindSuccessorResponseMessage m, ByteBuf out) {
            out.writeInt(m.getNodeId());
            out.writeInt(m.fingers.size());
            for(FingerEntry f : m.fingers){
                f.serialize(out);
            }
            m.predecessor.serialize(out);
        }

        @Override
        public FindSuccessorResponseMessage deserialize(ByteBuf in) throws UnknownHostException {
            int nodeId = in.readInt();
            int size = in.readInt();
            List<FingerEntry> list = new ArrayList<>(size);
            for(int i = 0 ; i< size; i++){
                list.add(FingerEntry.deserialize(in));
            }
            Host pre = Host.deserialize(in);

            return new FindSuccessorResponseMessage(nodeId,list,pre);
        }

        @Override
        public int serializedSize(FindSuccessorResponseMessage m) {
            return Integer.BYTES + m.fingers.size() * FingerEntry.serializedSize() + m.predecessor.serializedSize();;
        }
    };
}
