package protocols.dissemination.timers;

import babel.timer.ProtocolTimer;

public class RefreshSubscribesTimer extends ProtocolTimer {

    public static final short TimerCode = 22;

    public RefreshSubscribesTimer() {
        super(TimerCode);
    }

    @Override
    public Object clone() {
        return this;
    }
}
