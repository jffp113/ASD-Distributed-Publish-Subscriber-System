package protocols.dissemination;

import network.Host;

public class HostSubscription {

    private final Host host;
    private final long subscriptionTime;

    public HostSubscription(Host host, long subscriptionTime) {
        this.host = host;
        this.subscriptionTime = subscriptionTime;
    }

    public Host getHost() {
        return host;
    }

    public boolean isTimeExpired(long offset) {
        return System.currentTimeMillis() - this.subscriptionTime > offset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        HostSubscription that = (HostSubscription) o;
        return host.equals(that.host);
    }

    @Override
    public int hashCode() {
        return this.host.hashCode();
    }

    @Override
    public String toString() {
        return host.toString();
    }
}
