package protocols.dht.requests;

import babel.requestreply.ProtocolRequest;

public class RouteRequest extends ProtocolRequest {

    public static final short REQUEST_ID = 20234;
    private static final String PAYLOAD_SEPARATOR = "|";

    private String message;
    private int id;

    public RouteRequest(String message, int id) {
        super(RouteRequest.REQUEST_ID);
        this.message = message;
        this.id = id;
    }

    public String getMessage() {
        return message;
    }

    public int getid() {
        return id;
    }

}
