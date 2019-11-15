package protocols.dht;

import io.netty.buffer.ByteBuf;
import network.Host;

import java.net.UnknownHostException;

public class FingerEntry {

    private int start;
    private int hostId;
    private Host host;

    public FingerEntry(int start, int hostId, Host host) {
        this.start = start;
        this.hostId = hostId;
        this.host = host;
    }

    public Host getHost() {
        return host;
    }

    public int getStart() {
        return start;
    }

    public int getHostId() {
        return hostId;
    }

    public void setHostId(int hostId) {
        this.hostId = hostId;
    }

    public void setHost(Host host) {
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

    public FingerEntry clone() {
        return new FingerEntry(start, hostId, host);
    }

    @Override
    public String toString() {
        return String.format("Start: %d hostId: %d host %s", start,hostId,host);
    }
}
