package protocols.dissemination;

import network.Host;

public class HostSubscription implements Comparable<HostSubscription> {

    private final Host host;
    private final long subscriptionTime;

    public HostSubscription(Host host, long subscriptionTime) {
        this.host = host;
        this.subscriptionTime = subscriptionTime;
    }

    public Host getHost() {
        return host;
    }

    public boolean isTimeExpired(long offset){
        return System.currentTimeMillis() - this.subscriptionTime > offset;
    }

    @Override
    public int compareTo(HostSubscription o) {
        return this.host.compareTo(o.host);
    }
}
