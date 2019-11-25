package protocols.publishsubscribe.requests;

import babel.requestreply.ProtocolRequest;
import network.Host;

public class StartRequest extends ProtocolRequest {
    public static short REQUEST_ID = 13;
    private Host contact;

    public StartRequest(Host contact) {
        super(REQUEST_ID);
        this.contact = contact;
    }

    public StartRequest() {
        super(REQUEST_ID);
        this.contact = null;
    }

    public Host getContact() {
        return contact;
    }

    public void setContact(Host contact) {
        this.contact = contact;
    }
}
