package protocols.partialmembership.timers;

import babel.timer.ProtocolTimer;

public class GossipProtocolTimer extends ProtocolTimer {

    public static final short TimerCode = 101;

    public GossipProtocolTimer() {
        super(GossipProtocolTimer.TimerCode);
    }

    @Override
    public Object clone() {
        return this;
    }
}

