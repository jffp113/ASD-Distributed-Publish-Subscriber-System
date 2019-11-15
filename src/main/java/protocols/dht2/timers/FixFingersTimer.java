package protocols.dht2.timers;

import babel.timer.ProtocolTimer;

public class FixFingersTimer extends ProtocolTimer {

    public static final short TimerCode = 1031;

    public FixFingersTimer() {
        super(TimerCode);
    }

    @Override
    public Object clone() {
        return this;
    }
}
