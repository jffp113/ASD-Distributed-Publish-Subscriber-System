package protocols.dissemination.timers;

import babel.timer.ProtocolTimer;

public class RecycleSubscribesTimer extends ProtocolTimer {
    public static final short TimerCode = 21;

    public RecycleSubscribesTimer() {
        super(TimerCode);
    }

    @Override
    public Object clone() {
        return this;
    }
}
