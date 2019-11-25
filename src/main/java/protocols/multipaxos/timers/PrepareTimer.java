package protocols.multipaxos.timers;

import babel.timer.ProtocolTimer;

public class PrepareTimer extends ProtocolTimer {
    public static final short TIMER_CODE = 113;

    public PrepareTimer(int sequenceNumber) {
        super(PrepareTimer.TIMER_CODE);
    }

    @Override
    public Object clone() {
        return this;
    }
}
