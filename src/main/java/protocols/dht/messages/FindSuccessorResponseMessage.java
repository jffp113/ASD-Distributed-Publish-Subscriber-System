package protocols.dht.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.Host;
import network.ISerializer;
import protocols.dht.FingerEntry;

import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;

public class FindSuccessorResponseMessage extends ProtocolMessage {
    public static short MSG_CODE = 10001;

    private Host successor;

    private List<FingerEntry> fingerEntryList;


    public FindSuccessorResponseMessage(Host successor,List<FingerEntry> fingerEntryList) {
        super(MSG_CODE);
        this.successor = successor;
        this.fingerEntryList = fingerEntryList;
    }

    public Host getSuccessor() {
        return successor;
    }

    public List<FingerEntry> getFingerEntryList() {
        return fingerEntryList;
    }

    public static final ISerializer<FindSuccessorResponseMessage> serializer = new ISerializer<FindSuccessorResponseMessage>() {
        @Override
        public void serialize(FindSuccessorResponseMessage m, ByteBuf out) {
            m.successor.serialize(out);
            out.writeInt(m.fingerEntryList.size());
            for(FingerEntry f : m.fingerEntryList){
                f.serialize(out);
            }
        }

        @Override
        public FindSuccessorResponseMessage deserialize(ByteBuf in) throws UnknownHostException {
            Host successor = Host.deserialize(in);
            int a = in.readInt();
            List<FingerEntry> fingerEntryList = new LinkedList<>();
            for(int i = 0; i < a; i++){
                fingerEntryList.add(FingerEntry.deserialize(in));
            }

            return new FindSuccessorResponseMessage(successor,fingerEntryList);
        }

        @Override
        public int serializedSize(FindSuccessorResponseMessage m) {
            return m.successor.serializedSize() + Integer.BYTES + m.fingerEntryList.size()*FingerEntry.serializedSize();
        }
    };
}
