package protocols.floadbroadcastrecovery.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.Host;
import network.ISerializer;

import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

public class ReBCastProtocolMessage extends ProtocolMessage {

    public final static short MSG_CODE = 2016;

    private final List<UUID> midList;
    private final UUID mid;
    private final Host initialHost;

    public ReBCastProtocolMessage(List<UUID> midList,UUID mid,Host initialHost) {
        super(ReBCastProtocolMessage.MSG_CODE);
        this.mid = mid;
        this.midList = midList;
        this.initialHost = initialHost;
    }

    public ReBCastProtocolMessage(List<UUID> midList, Host initialHost) {
       this(midList, UUID.randomUUID(),initialHost);
    }

    public UUID getMessageId() {
        return mid;
    }

    public List<UUID> getMessageUUIDList() {
        return midList;
    }

    public Host getInitialHost() {
        return initialHost;
    }

    public static final ISerializer<ReBCastProtocolMessage> serializer = new ISerializer<ReBCastProtocolMessage>() {
        @Override
        public void serialize(ReBCastProtocolMessage m, ByteBuf out) {
            m.initialHost.serialize(out);
            serializeUUID(m.mid,out);
            out.writeInt(m.midList.size());
            for(UUID mid : m.midList){
                serializeUUID(mid,out);
            }
        }

        private void serializeUUID(UUID mid,ByteBuf out){
            out.writeLong(mid.getMostSignificantBits());
            out.writeLong(mid.getLeastSignificantBits());
        }

        @Override
        public ReBCastProtocolMessage deserialize(ByteBuf in) throws UnknownHostException{
            Host h = Host.deserialize(in);
            List<UUID> midList = new LinkedList<>();
            UUID mid = deserializeUUDI(in);
            int i = in.readInt();
            for(;i > 0; i--){
                midList.add(deserializeUUDI(in));
            }
            return new ReBCastProtocolMessage(midList,mid,h);
        }

        private UUID deserializeUUDI(ByteBuf in){
            return new UUID(in.readLong(), in.readLong());
        }

        @Override
        public int serializedSize(ReBCastProtocolMessage m) {
            int hostSize=6;
            return (m.midList.size() + 1)*2*Long.BYTES + hostSize + Integer.BYTES;
        }
    };
}
