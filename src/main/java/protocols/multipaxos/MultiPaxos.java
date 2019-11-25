package protocols.multipaxos;

import babel.exceptions.HandlerRegistrationException;
import babel.handlers.ProtocolMessageHandler;
import babel.handlers.ProtocolRequestHandler;
import babel.handlers.ProtocolTimerHandler;
import babel.protocol.GenericProtocol;
import babel.protocol.event.ProtocolMessage;
import network.Host;
import network.INetwork;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.omg.CORBA.PUBLIC_MEMBER;
import persistence.PersistentMap;
import protocols.multipaxos.messages.*;
import protocols.multipaxos.timers.PrepareTimer;
import protocols.publishsubscribe.PublishSubscribe;
import protocols.publishsubscribe.requests.ExecuteOperationRequest;
import protocols.publishsubscribe.requests.StartRequest;
import protocols.publishsubscribe.requests.StartRequestReply;

import java.util.*;

public class MultiPaxos extends GenericProtocol {

    public static final short PROTOCOL_ID = 1243;
    public static final String PROTOCOL_NAME = "MultiPaxos";
    private static final int INITIAL_SEQUENCE_NUMBER = 0;

    static Logger logger = LogManager.getLogger(MultiPaxos.class.getName());
    private Host leader;


    public MultiPaxos(INetwork net) throws HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID, net);

        registerRequestHandler(StartRequest.REQUEST_ID, uponStartRequest);

        registerMessageHandler(AddReplicaMessage.MSG_CODE, uponAddReplicaMessage, AddReplicaMessage.serializer);
        registerMessageHandler(PrepareMessage.MSG_CODE, uponPrepareMessage, PrepareMessage.serializer);
        registerMessageHandler(PrepareOk.MSG_CODE, uponPrepareOk, PrepareOk.serializer);
        registerMessageHandler(OperationMessage.MSG_CODE, uponOperation, OperationMessage.serializer);
        registerTimerHandler(PrepareTimer.TIMER_CODE, uponPrepareTimer);
    }

    @Override
    public void init(Properties properties) {
        this.leader = null;
        this.replicaSet = new HashSet<>();
        this.pendingPrepares = new HashMap<>();
        this.sequenceNumber = INITIAL_SEQUENCE_NUMBER;
        this.highestPrepare = INITIAL_SEQUENCE_NUMBER;
        this.pendingOperations = new HashMap<>();
        //this.operationsToExecute = new LinkedList<>();
        this.prepareTimout = 0;
    }


    private int operationAcceptSequeceNumber = 0;
    private final ProtocolMessageHandler uponOperation = (protocolMessage) -> {
        OperationMessage operation = (OperationMessage)protocolMessage;
        sendMessageToReplicaSet(new AcceptOperationMessage(++operationAcceptSequeceNumber,operation.getOperation()));
    };


    private int currentExecutedOperation = 0;
    private final ProtocolMessageHandler uponAcceptOperation = (protocolMessage) -> {
        AcceptOperationMessage operation = (AcceptOperationMessage)protocolMessage;

        ExecuteOperationRequest exop = new ExecuteOperationRequest(operation.getSequenceNumber(),operation.getOperation());
        exop.setDestination(PublishSubscribe.PROTOCOL_ID);
        deliverRequest(exop);

    };

    private final ProtocolMessageHandler uponAddReplicaMessage = (protocolMessage) -> {
        AddReplicaMessage message = (AddReplicaMessage) protocolMessage;
        if (imLeader()) {
            // propose nao sei o que
            logger.info("Im leader processing request from %s\n", message.getRequester());
        } else {
            sendMessage(message, this.leader);
            logger.info("Forwarding message to leader %s\n", this.leader);
        }
    };
    private Set<Host> replicaSet;
    // Proposer
    private int highestPrepare;
    private int sequenceNumber;
    private final ProtocolRequestHandler uponStartRequest = (protocolRequest) -> {
        StartRequest request = (StartRequest) protocolRequest;
        Host contact = request.getContact();

        if (contact == null) {
            this.leader = myself;
            this.replicaSet.add(myself);

            StartRequestReply reply = new StartRequestReply(this.replicaSet, this.leader, this.sequenceNumber);
            reply.setDestination(PublishSubscribe.PROTOCOL_ID);
            deliverReply(reply);
        } else {
            AddReplicaMessage message = new AddReplicaMessage(myself);
            sendMessageSideChannel(message, contact);
        }

    };
    // Acceptor
    private int highestAccept;
    // guardar para cada proposed operation o resultado dos prepareoks para quando tiver a maioria selecionar a op a executar
    private Operation valueAccepted;

    private final ProtocolMessageHandler uponPrepareMessage = (protocolMessage) -> {
        PrepareMessage message = (PrepareMessage) protocolMessage;
        int seqNum = message.getSequenceNumber();
        if (seqNum > this.highestPrepare) {
            this.highestPrepare = seqNum;
            PrepareOk prepareOk = new PrepareOk(this.highestAccept, this.valueAccepted);
            sendMessage(prepareOk, message.getFrom());
        }
    };


    private Map<Integer, Operation> pendingOperations;
    private Map<Integer, List<Operation>> pendingPrepares;

    private final ProtocolMessageHandler uponPrepareOk = (protocolMessage) -> {
        PrepareOk message = (PrepareOk) protocolMessage;
        int acceptorSeqNumber = message.getSequenceNumber();
        List<Operation> prepareOkReply = this.pendingPrepares.get(sequenceNumber);

        if (prepareOkReply != null) {
            prepareOkReply.add(message.getOperation());
            if (hasMajority(prepareOkReply.size())) {
                this.pendingPrepares.remove(sequenceNumber);

                Operation highestOperation = pickHighestOperation(prepareOkReply);
            }
        }

    };
    private int prepareTimout;
    private final ProtocolTimerHandler uponPrepareTimer = (protocolTimer) -> {
        PrepareTimer timer = (PrepareTimer) protocolTimer;
        int seqNum = timer.getSequenceNumber();

        List<Operation> numPrepareOk = this.pendingPrepares.remove(seqNum);
        if (numPrepareOk != null) {
            sendPrepare(this.pendingOperations.get(sequenceNumber));
        }

    };



    private Operation pickHighestOperation(List<Operation> operations) {
        Map<Operation,Integer> result = new HashMap<>();
        int hSeq = Integer.MIN_VALUE;

        for(Operation op : operations){
            Integer occ = result.get(op);
            if(occ == null){
                occ = new Integer(0);
            }
            occ++;
            result.put(op,occ);
            if(occ > hSeq){
                hSeq = occ;
            }
            if((operations.size() / 2) + 1 <= occ){
                return op;
            }
        }
        Operation o = operations.get(0);
        o.setSeq(hSeq);
        return o;
    }

    private boolean imLeader() {
        return myself.equals(this.leader);
    }

    private void sendPrepare(Operation operation) {
        List<Operation> opList = new LinkedList<>();
        this.sequenceNumber = getNextSequenceNumber();
        logger.info("[%s] sending prepare with sequence number %s\n", myself, this.sequenceNumber);
        setupTimer(new PrepareTimer(this.sequenceNumber), prepareTimout);
        opList.add(operation);
        this.pendingPrepares.put(this.sequenceNumber, opList);
        //this.pendingOperations.put(this.sequenceNumber, operation);
        sendMessageToReplicaSet(new PrepareMessage(this.sequenceNumber));
    }

    private int getNextSequenceNumber() {
        int seqNum = this.sequenceNumber;
        while (seqNum < highestPrepare) {
            seqNum += replicaSet.size();
        }

       // (highestPrepare - seqNum)/replicaSet.size() =  n

        return seqNum;
    }

    private void sendMessageToReplicaSet(ProtocolMessage message) {
        for (Host replica : replicaSet) {
            sendMessage(message, replica);
        }
    }

    private boolean hasMajority(int awnsers) {
        int replicaSize = this.replicaSet.size();
        if (replicaSize <= 2) {
            return number == replicaSize;
        }

        return number > (replicaSize / 2) + 1;
    }
}
