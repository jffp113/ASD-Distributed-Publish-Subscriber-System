package protocols.partialmembership.timers;

import babel.timer.ProtocolTimer;

public class EchoProtocolTimer extends ProtocolTimer {

    public static final short TimerCode = 102;

    public EchoProtocolTimer() {
        super(EchoProtocolTimer.TimerCode);
    }

    @Override
    public Object clone() {
        return this;
    }
}
