package protocols.dht;

import io.netty.buffer.ByteBuf;
import network.Host;
import network.ISerializer;
import protocols.dht.messages.FingerTableResponseMessage;

import java.io.ByteArrayInputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

public class FingerEntry {
    public int start;
    public int begin;
    public int end;
    public int suc;
    public Host h;

    public FingerEntry(int start, int begin, int end, int suc, Host h) {
        this.start = start;
        this.begin = begin;
        this.end = end;
        this.suc = suc;
        this.h = h;
    }


    public static FingerEntry deserialize(String fingerEntryAsString) throws Exception {
        String[] elements = fingerEntryAsString.split("\n");
        String[] contactSplit = elements[4].split(":");
        Host host = new Host(InetAddress.getByName(contactSplit[0]), Integer.parseInt(contactSplit[1]));
        return new FingerEntry(Integer.parseInt(elements[0]),
                                Integer.parseInt(elements[1]),
                                Integer.parseInt(elements[2]),
                                Integer.parseInt(elements[3]),
                                host);
    }

    public void serialize(ByteBuf out) {
        out.writeInt(start);
        out.writeInt(begin);
        out.writeInt(end);
        out.writeInt(suc);
        h.serialize(out);
    }

    public static FingerEntry deserialize(ByteBuf in) throws UnknownHostException {
        return new FingerEntry(in.readInt(),
                                in.readInt(),
                                in.readInt(),
                                in.readInt(),
                        Host.deserialize(in));
    }

    public static int serializedSize() {
        return 4 * Integer.BYTES + 6;
    }
}
