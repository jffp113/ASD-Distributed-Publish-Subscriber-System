package protocols.floadbroadcastrecovery.timers;

import babel.timer.ProtocolTimer;

public class PeriodicRebroadcastProtocolTimer extends ProtocolTimer {

    public static final short TIMER_CODE = 1020;

    public PeriodicRebroadcastProtocolTimer() {
        super(PeriodicRebroadcastProtocolTimer.TIMER_CODE);
    }

    @Override
    public Object clone() {
        return this;
    }
}
