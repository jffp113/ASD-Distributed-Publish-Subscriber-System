package protocols.floadbroadcastrecovery.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;

import java.io.Serializable;
import java.net.UnknownHostException;
import java.util.UUID;

public class BCastProtocolMessage extends ProtocolMessage implements Serializable {

    public final static short MSG_CODE = 201;

    private final UUID mid;
    private byte[] payload;


    public BCastProtocolMessage() {
        super(BCastProtocolMessage.MSG_CODE);
        this.mid = UUID.randomUUID();
        this.payload = new byte[0];

    }

    public BCastProtocolMessage(UUID mid, byte[] payload) {
        super(BCastProtocolMessage.MSG_CODE);
        this.mid = mid;
        this.payload = payload;
    }

    public void setPayload(byte[] payload) {
        this.payload = new byte[payload.length];
        System.arraycopy(payload, 0, this.payload, 0, payload.length);
    }

    public int getLength() {
        return this.payload.length;
    }

    public byte[] getPayload() {
        return this.payload;
    }

    public UUID getMessageId() {
        return mid;
    }

    public static final ISerializer<BCastProtocolMessage> serializer = new ISerializer<BCastProtocolMessage>() {
        @Override
        public void serialize(BCastProtocolMessage m, ByteBuf out) {
            out.writeLong(m.mid.getMostSignificantBits());
            out.writeLong(m.mid.getLeastSignificantBits());
            out.writeInt(m.payload.length);
            out.writeBytes(m.payload);
        }

        @Override
        public BCastProtocolMessage deserialize(ByteBuf in) throws UnknownHostException {
            UUID mid = new UUID(in.readLong(), in.readLong());
            int size = in.readInt();
            byte[] payload = new byte[size];
            in.readBytes(payload);
            return new BCastProtocolMessage(mid, payload);
        }

        @Override
        public int serializedSize(BCastProtocolMessage m) {
            return (2*Long.BYTES) + Integer.BYTES + m.payload.length;
        }
    };
}
