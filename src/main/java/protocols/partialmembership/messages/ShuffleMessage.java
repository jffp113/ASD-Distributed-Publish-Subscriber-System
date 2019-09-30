package protocols.partialmembership.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.Host;
import network.ISerializer;

import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class ShuffleMessage extends ProtocolMessage {
    public final static short MSG_CODE = 1111;


    public static final ISerializer<ShuffleMessage> serializer = new ISerializer<ShuffleMessage>() {
        @Override
        public void serialize(ShuffleMessage shuffleRequest, ByteBuf out) {
            out.writeLong(shuffleRequest.mid.getMostSignificantBits());
            out.writeLong(shuffleRequest.mid.getLeastSignificantBits());
            out.writeInt(shuffleRequest.ttl);
            out.writeShort(shuffleRequest.avSample.size());
            out.writeShort(shuffleRequest.pvSample.size());

            for (Host h : shuffleRequest.avSample) {
                h.serialize(out);
            }

            for (Host h : shuffleRequest.pvSample) {
                h.serialize(out);
            }

            shuffleRequest.sender.serialize(out);
        }

        @Override
        public ShuffleMessage deserialize(ByteBuf in) throws UnknownHostException {
            UUID mid = new UUID(in.readLong(), in.readLong());

            short avSampleSize = in.readShort();
            short pvSampleSize = in.readShort();
            int ttl = in.readInt();

            Set<Host> avSample = new HashSet<>();
            Set<Host> pvSample = new HashSet<>();

            for (int i = 0; i < avSampleSize; i++) {
                avSample.add(Host.deserialize(in));
            }

            for (int i = 0; i < pvSampleSize; i++) {
                pvSample.add(Host.deserialize(in));
            }

            Host sender = Host.deserialize(in);

            return new ShuffleMessage(mid, avSample, pvSample, sender, ttl);
        }

        @Override
        public int serializedSize(ShuffleMessage shuffleRequest) {
            Host sender = shuffleRequest.sender;
            return 2*Long.BYTES + 2 * Short.BYTES + (shuffleRequest.pvSample.size() + shuffleRequest.avSample.size() + 1)
                    * sender.serializedSize() + Integer.BYTES;
        }
    };
    private Set<Host> avSample;
    private Set<Host> pvSample;
    private UUID mid;
    private Host sender;
    private int ttl;

    public ShuffleMessage(Set<Host> avSample, Set<Host> pvSample, Host sender, int ttl) {
        this(UUID.randomUUID(), avSample, pvSample, sender, ttl);
    }

    public ShuffleMessage(UUID mid, Set<Host> avSample, Set<Host> pvSample, Host sender, int ttl) {
        super(ShuffleMessage.MSG_CODE);

        this.avSample = avSample;
        this.pvSample = pvSample;
        this.mid = mid;
        this.sender = sender;
        this.ttl = ttl;
    }

    @Override
    public String toString() {
        return "ShuffleMessage{" +
                "uuid=" + mid +
                "avSample=" + avSample +
                "pvSample=" + pvSample +
                "sender=" + sender +
                '}';
    }

    public Set<Host> getAvSample() {
        return avSample;
    }

    public Set<Host> getPvSample() {
        return pvSample;
    }

    public Host getSender() {
        return sender;
    }

    public int getTtl() {
        return ttl;
    }

    public int decTtl() {
        ttl--;
        return ttl;
    }

    public UUID getMid() {
        return mid;
    }
}
