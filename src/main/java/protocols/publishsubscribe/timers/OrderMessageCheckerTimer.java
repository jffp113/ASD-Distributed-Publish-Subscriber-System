package protocols.publishsubscribe.timers;

import babel.timer.ProtocolTimer;

public class OrderMessageCheckerTimer extends ProtocolTimer {

    public static final short TimerCode = 6666;

    public OrderMessageCheckerTimer() {
        super(TimerCode);
    }

    @Override
    public Object clone() {
        return this;
    }

}
