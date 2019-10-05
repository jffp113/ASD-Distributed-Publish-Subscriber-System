package protocols.floadbroadcastrecovery.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;

import java.util.UUID;

public class MessageRequestProtocolMessage extends ProtocolMessage {

    public final static short MSG_CODE = 2201;
    private final UUID mid;

    public MessageRequestProtocolMessage(UUID mid) {
        super(MessageRequestProtocolMessage.MSG_CODE);
        this.mid = mid;
    }

    public UUID getMessageId() {
        return mid;
    }

    public static final ISerializer<MessageRequestProtocolMessage> serializer = new ISerializer<MessageRequestProtocolMessage>() {
        @Override
        public void serialize(MessageRequestProtocolMessage m, ByteBuf out) {
            serializeUUID(m.mid,out);
        }

        private void serializeUUID(UUID mid,ByteBuf out){
            out.writeLong(mid.getMostSignificantBits());
            out.writeLong(mid.getLeastSignificantBits());
        }

        @Override
        public MessageRequestProtocolMessage deserialize(ByteBuf in) {
            UUID mid = deserializeUUDI(in);
            return new MessageRequestProtocolMessage(mid);
        }

        private UUID deserializeUUDI(ByteBuf in){
            return new UUID(in.readLong(), in.readLong());
        }

        @Override
        public int serializedSize(MessageRequestProtocolMessage m) {
            return (2*Long.BYTES);
        }
    };
}
