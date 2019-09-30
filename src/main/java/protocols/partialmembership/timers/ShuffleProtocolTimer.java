package protocols.partialmembership.timers;

import babel.timer.ProtocolTimer;

public class ShuffleProtocolTimer extends ProtocolTimer {
    public static final short TIMERCODE = 101;

    public ShuffleProtocolTimer() {
        super(ShuffleProtocolTimer.TIMERCODE);
    }

    @Override
    public Object clone() {
        return this;
    }
}
