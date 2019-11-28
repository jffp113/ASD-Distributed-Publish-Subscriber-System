package protocols.multipaxos.requests;

import babel.requestreply.ProtocolRequest;
import protocols.multipaxos.Operation;

public class ProposeRequest extends ProtocolRequest {
    public static final short REQUEST_ID = 22222;

    private Operation operation;

    public ProposeRequest(Operation operation) {
        super(REQUEST_ID);
        this.operation = operation;
    }

    public Operation getOperation() {
        return operation;
    }

}
