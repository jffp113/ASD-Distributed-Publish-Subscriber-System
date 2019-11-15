package protocols.dht2.timers;

import babel.timer.ProtocolTimer;

public class StabilizeTimer extends ProtocolTimer {

    public static final short TimerCode = 1030;

    public StabilizeTimer() {
        super(StabilizeTimer.TimerCode);
    }

    @Override
    public Object clone() {
        return this;
    }
}
