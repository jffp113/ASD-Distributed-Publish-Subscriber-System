package protocols.dht.timers;

import babel.timer.ProtocolTimer;

public class RefreshTopics extends ProtocolTimer {
    public static final short TimerCode = 21;

    public RefreshTopics() {
        super(TimerCode);
    }

    @Override
    public Object clone() {
        return this;
    }

}
