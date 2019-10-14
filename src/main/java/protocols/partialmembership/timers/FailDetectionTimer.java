package protocols.partialmembership.timers;

import babel.timer.ProtocolTimer;
import network.Host;

public class FailDetectionTimer extends ProtocolTimer {

    public static final short TimerCode = 10123;
    private Host host;

    public FailDetectionTimer(Host host) {
        super(FailDetectionTimer.TimerCode);
        this.host = host;
    }

    public Host getHost() {
        return host;
    }

    @Override
    public Object clone() {
        return this;
    }
}
