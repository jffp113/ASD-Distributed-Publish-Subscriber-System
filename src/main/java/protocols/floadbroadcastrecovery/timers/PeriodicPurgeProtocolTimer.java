package protocols.floadbroadcastrecovery.timers;

import babel.timer.ProtocolTimer;

public class PeriodicPurgeProtocolTimer extends ProtocolTimer {

    public static final short TIMER_CODE = 1023;

    public PeriodicPurgeProtocolTimer() {
        super(PeriodicPurgeProtocolTimer.TIMER_CODE);
    }

    @Override
    public Object clone() {
        return this;
    }
}
