package protocols.floadbroadcastrecovery.requests;

import babel.requestreply.ProtocolRequest;

public class BCastRequest extends ProtocolRequest {

    public static final short REQUEST_ID = 201;

    private byte[] payload;

    public BCastRequest(byte[] message) {
        super(BCastRequest.REQUEST_ID);
        if(message != null) {
            this.payload = new byte[message.length];
            System.arraycopy(message, 0, this.payload, 0, message.length);
        } else {
            this.payload = new byte[0];
        }
    }

    public byte[] getPayload() {
        return payload;
    }
}
