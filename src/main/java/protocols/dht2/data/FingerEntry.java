package protocols.dht2.data;

import io.netty.buffer.ByteBuf;
import network.Host;

import java.net.UnknownHostException;

public class FingerEntry {

    private ID startId;
    private ID hostId;
    private Host host;

    public FingerEntry(ID start, Host host) {
        this.startId = start;
        this.hostId = new ID(host);
        this.host = host;
    }

    public static FingerEntry genFingerEntryToHost(ID hostId,Host host,int pos){
        return new FingerEntry(hostId.sumID(new ID((int)Math.pow(2, pos))),host);

    }

    public Host getHost() {
        return host;
    }

    public ID getStartId() {
        return startId;
    }

    public ID getHostId() {
        return hostId;
    }

    protected void setHost(Host host) {
        this.hostId = new ID(host);
        this.host = host;
    }

    public void serialize(ByteBuf out) {
        startId.serialize(out);
        host.serialize(out);
    }

    public static FingerEntry deserialize(ByteBuf in) throws UnknownHostException {
        return new FingerEntry(ID.deserialize(in), Host.deserialize(in));
    }

    public static int serializedSize() {
        return 2 * ID.serializeSize() + 6;
    }

    public FingerEntry clone() {
        return new FingerEntry(startId, host);
    }

    @Override
    public String toString() {
        return String.format("Start: %d hostId: %d host %s", startId,hostId,host);
    }
}
