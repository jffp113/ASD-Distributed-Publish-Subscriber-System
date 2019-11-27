package utils;

import java.util.concurrent.atomic.AtomicInteger;

public class Utils {
    private static AtomicInteger idCounter = new AtomicInteger();
    private static AtomicInteger seqCounter = new AtomicInteger(1);

    public static int generateId() {
        return idCounter.incrementAndGet();
    }

    public static int generateSeqNum() {
        return seqCounter.incrementAndGet();
    }
}
