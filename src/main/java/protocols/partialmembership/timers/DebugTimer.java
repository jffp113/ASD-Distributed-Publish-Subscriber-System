package protocols.partialmembership.timers;

import babel.timer.ProtocolTimer;

public class DebugTimer extends ProtocolTimer {

    public static final short TimerCode = 1020;

    public DebugTimer() {
        super(DebugTimer.TimerCode);
    }

    @Override
    public Object clone() {
        return this;
    }
}
