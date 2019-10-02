package protocols.floadbroadcastrecovery.timers;

import babel.timer.ProtocolTimer;

public class PeriodicRebroadcastProtocolTimer extends ProtocolTimer {

    public static final short TimerCode = 1020;

    public PeriodicRebroadcastProtocolTimer() {
        super(PeriodicRebroadcastProtocolTimer.TimerCode);
    }

    @Override
    public Object clone() {
        return this;
    }
}
