package protocols.multipaxos.timers;

import babel.timer.ProtocolTimer;

public class NoOpTimeout extends ProtocolTimer {
    public static final short TIMER_CODE = 223;

    public NoOpTimeout() {
        super(TIMER_CODE);
    }

    @Override
    public Object clone() {
        return this;
    }

}
