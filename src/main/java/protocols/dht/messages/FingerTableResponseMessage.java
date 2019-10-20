package protocols.dht.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.Host;
import network.ISerializer;
import protocols.dht.FingerEntry;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;


public class FingerTableResponseMessage extends ProtocolMessage {

    public final static short MSG_CODE = 23481;
    private List<FingerEntry> fingers;
    private Host predecessor;

    public FingerTableResponseMessage(List<FingerEntry> fingers, Host predecessor) {
        super(FingerTableResponseMessage.MSG_CODE);
        this.fingers = fingers;
        this.predecessor = predecessor;
    }

    public Host getPredecessor() {
        return predecessor;
    }

    public List<FingerEntry> getFingers() {
        return fingers;
    }


    public static final ISerializer<FingerTableResponseMessage> serializer = new ISerializer<FingerTableResponseMessage>() {
        @Override
        public void serialize(FingerTableResponseMessage m, ByteBuf out) {
            out.writeInt(m.fingers.size());
            for(FingerEntry f : m.fingers){
               f.serialize(out);
            }
            m.predecessor.serialize(out);
        }

        @Override
        public FingerTableResponseMessage deserialize(ByteBuf in) throws UnknownHostException {
            int size = in.readInt();
            List<FingerEntry> list = new ArrayList<>(size);
            for(int i = 0 ; i< size; i++){
                list.add(FingerEntry.deserialize(in));
            }
            Host pre = Host.deserialize(in);

            return new FingerTableResponseMessage(list,pre);
        }

        @Override
        public int serializedSize(FingerTableResponseMessage m) {
            return m.fingers.size() * FingerEntry.serializedSize() + m.predecessor.serializedSize();
        }
    };

}
