package protocols.publishsubscribe.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;

import java.net.UnknownHostException;

public class LatePaxosInstanceRequestMessage extends ProtocolMessage {

    public final static short MSG_CODE = 10223;

    private int start;
    private int end;

    public static final ISerializer<LatePaxosInstanceRequestMessage> serializer = new ISerializer<LatePaxosInstanceRequestMessage>() {
        @Override
        public void serialize(LatePaxosInstanceRequestMessage m, ByteBuf out) {
            out.writeInt(m.start);
            out.writeInt(m.end);

        }

        @Override
        public LatePaxosInstanceRequestMessage deserialize(ByteBuf in) throws UnknownHostException {
            int start = in.readInt();
            int end = in.readInt();
            return new LatePaxosInstanceRequestMessage(start,end);
        }

        @Override
        public int serializedSize(LatePaxosInstanceRequestMessage m) {
            return Integer.BYTES * 2;
        }
    };

    public LatePaxosInstanceRequestMessage(int start, int end) {
        super(MSG_CODE);
        this.start = start;
        this.end = end;
    }

    public int getEnd() {
        return end;
    }

    public int getStart() {
        return start;
    }


}
