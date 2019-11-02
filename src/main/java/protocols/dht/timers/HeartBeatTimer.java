package protocols.dht.timers;

import babel.timer.ProtocolTimer;
import network.Host;

public class HeartBeatTimer extends ProtocolTimer {

    public static final short TimerCode = 20131;

    private Host host;
    private int pos;

    public HeartBeatTimer(Host host, int pos) {
        super(TimerCode);
        this.host = host;
    }

    @Override
    public Object clone() {
        return this;
    }

    public Host getHost() {
        return host;
    }

    public int getPos() {
        return pos;
    }
}
