package protocols.multipaxos.timers;

import babel.timer.ProtocolTimer;

public class PrepareTimer extends ProtocolTimer {
    public static final short TIMER_CODE = 113;

    private int sequenceNumber;

    public PrepareTimer(int sequenceNumber) {
        super(PrepareTimer.TIMER_CODE);
        this.sequenceNumber = sequenceNumber;
    }

    public int getSequenceNumber() {
        return sequenceNumber;
    }

    @Override
    public Object clone() {
        return this;
    }
}
