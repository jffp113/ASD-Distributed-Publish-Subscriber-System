package protocols.dht;

import io.netty.buffer.ByteBuf;
import network.Host;

import java.net.UnknownHostException;

public class FingerEntry {


    public int start;
    public int hostId;
    public Host host;

    public FingerEntry(int start, int hostId, Host host) {
        this.start = start;
        this.hostId = hostId;
        this.host = host;
    }

    public void serialize(ByteBuf out) {
        out.writeInt(start);
        out.writeInt(hostId);
        host.serialize(out);
    }

    public static FingerEntry deserialize(ByteBuf in) throws UnknownHostException {
        return new FingerEntry(in.readInt(), in.readInt(), Host.deserialize(in));
    }

    public static int serializedSize() {
        return 2 * Integer.BYTES + 6;
    }


    @Override
    public String toString() {
        return String.format("Start: %d hostId: %d host %s", start,hostId,host);
    }
}
