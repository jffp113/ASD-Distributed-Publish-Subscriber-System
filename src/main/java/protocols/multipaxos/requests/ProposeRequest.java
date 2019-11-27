package protocols.multipaxos.requests;

import babel.requestreply.ProtocolRequest;
import protocols.floadbroadcastrecovery.requests.BCastRequest;
import protocols.multipaxos.Operation;

public class ProposeRequest extends ProtocolRequest {
    public static final short REQUEST_ID = 22222;

    private Operation operation;

    public ProposeRequest(Operation operation) {
        super(BCastRequest.REQUEST_ID);
        this.operation = operation;
    }

    public Operation getOperation() {
        return operation;
    }
}
