package protocols.dht;

import network.Host;

public class Util {

    public static final int k = 64;
    public static final int fingers = 8;

    public static int calculateFinger(int myId, int iFinger){
        iFinger--;
        return (int)((myId + Math.pow(2,iFinger)) % k);
    }

    public static int calculateID(String seed){
        return Math.abs(seed.hashCode() % k);
    }

    public static int calculateIDByHost(Host h){
        return calculateID(h.toString());
    }
}
