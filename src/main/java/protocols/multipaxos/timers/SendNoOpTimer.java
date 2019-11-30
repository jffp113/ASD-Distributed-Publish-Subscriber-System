package protocols.multipaxos.timers;

import babel.timer.ProtocolTimer;

public class SendNoOpTimer extends ProtocolTimer {
    public static final short TIMER_CODE = 143;

    public SendNoOpTimer() {
        super(TIMER_CODE);
    }

    @Override
    public Object clone() {
        return this;
    }
}
