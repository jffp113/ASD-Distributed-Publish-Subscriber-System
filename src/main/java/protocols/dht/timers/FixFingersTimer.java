package protocols.dht.timers;

import babel.timer.ProtocolTimer;

public class FixFingersTimer extends ProtocolTimer {

    public static final short TimerCode = 1031;

    private int next;

    public FixFingersTimer(int next) {
        super(TimerCode);
        this.next = next;
    }

    public int getNext() {
        return next;
    }

    @Override
    public Object clone() {
        return this;
    }
}
