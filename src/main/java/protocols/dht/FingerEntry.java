package protocols.dht;

import io.netty.buffer.ByteBuf;
import network.Host;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class FingerEntry {


    public int start;
    public int end;
    public int hostId;
    public Host host;

    public FingerEntry(int start, int end, int hostId, Host host) {
        this.start = start;
        this.end = end;
        this.hostId = hostId;
        this.host = host;
    }

    public void serialize(ByteBuf out) {
        out.writeInt(start);
        out.writeInt(end);
        out.writeInt(hostId);
        host.serialize(out);
    }

    public static FingerEntry deserialize(ByteBuf in) throws UnknownHostException {
        return new FingerEntry(in.readInt(), in.readInt(), in.readInt(), Host.deserialize(in));
    }

    public static int serializedSize() {
        return 3 * Integer.BYTES + 6;
    }


    @Override
    public String toString() {
        return String.format("Start: %d End: %d hostId: %d host %s", start,end,hostId,host);
    }
}
