package protocols.multipaxos;

import babel.exceptions.HandlerRegistrationException;
import babel.handlers.ProtocolMessageHandler;
import babel.handlers.ProtocolRequestHandler;
import babel.handlers.ProtocolTimerHandler;
import babel.protocol.GenericProtocol;
import babel.protocol.event.ProtocolMessage;
import network.Host;
import network.INetwork;
import network.INodeListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import persistence.PersistentMap;
import protocols.multipaxos.messages.*;
import protocols.multipaxos.notifications.LeaderNotification;
import protocols.multipaxos.timers.PrepareTimer;
import protocols.publishsubscribe.PublishSubscribe;
import protocols.publishsubscribe.requests.ExecuteOperationRequest;
import protocols.publishsubscribe.requests.StartRequest;
import protocols.publishsubscribe.requests.StartRequestReply;
import utils.PropertiesUtils;

import java.util.*;

public class MultiPaxos extends GenericProtocol implements INodeListener {
    public static final short PROTOCOL_ID = 1243;
    private static final String PROTOCOL_NAME = "MultiPaxos";

    private static final String PREPARE_TIMEOUT = "prepareTimeout";
    private static final String KEY = "Operations";

    static Logger logger = LogManager.getLogger(MultiPaxos.class.getName());

    private int mySequenceNumber;
    private Host leader;
    private int leaderSN;
    private final ProtocolMessageHandler uponPrepareMessage = (protocolMessage) -> {
        PrepareMessage message = (PrepareMessage) protocolMessage;
        this.leader = message.getFrom();
        // TODO preciso de guardar o sequenceNumber do lider
        this.leaderSN = message.getSequenceNumber();

        // TODO verificar se mando as pending operations nesta camada ou na de cima
        PrepareOk prepareOk = new PrepareOk(message.getSequenceNumber(), null);
        sendMessage(prepareOk, message.getFrom());
        deliverNotification(new LeaderNotification(this.leader, this.leaderSN));
    };
    private int prepareOks;
    private boolean prepareIssued;
    private int paxosInstance;
    private Set<Host> replicaSet;
    private final ProtocolRequestHandler uponStartRequest = (protocolRequest) -> {
        StartRequest request = (StartRequest) protocolRequest;
        Host contact = request.getContact();

        if (contact == null) {
            this.leader = myself;
            this.replicaSet.add(myself);

            StartRequestReply reply = new StartRequestReply(this.replicaSet, this.leader, this.mySequenceNumber);
            reply.setDestination(PublishSubscribe.PROTOCOL_ID);
            deliverReply(reply);
        } else {
            AddReplicaMessage message = new AddReplicaMessage(myself);
            sendMessageSideChannel(message, contact);
        }

    };

    public MultiPaxos(INetwork net) throws HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID, net);

        registerRequestHandler(StartRequest.REQUEST_ID, uponStartRequest);

        registerMessageHandler(AddReplicaMessage.MSG_CODE, uponAddReplicaMessage, AddReplicaMessage.serializer);
        registerMessageHandler(PrepareMessage.MSG_CODE, uponPrepareMessage, PrepareMessage.serializer);
        registerMessageHandler(PrepareOk.MSG_CODE, uponPrepareOk, PrepareOk.serializer);
        registerMessageHandler(OperationMessage.MSG_CODE, uponOperation, OperationMessage.serializer);
        registerTimerHandler(PrepareTimer.TIMER_CODE, uponPrepareTimer);
    }
    private final ProtocolMessageHandler uponPrepareOk = (protocolMessage) -> {
        PrepareOk message = (PrepareOk) protocolMessage;

        if (this.mySequenceNumber == message.getSequenceNumber()) {
            prepareOks++;
        }

        if (hasMajority(prepareOks)) {
            Host oldLeader = this.leader;
            this.leader = myself;
            this.prepareOks = 0;
            this.prepareIssued = false;

            // TODO issue the operation to remove lider
            removeReplica(oldLeader);
        }


    };
    private final ProtocolTimerHandler uponPrepareTimer = (protocolTimer) -> {
        if (prepareIssued) {
            sendPrepare();
        }
    };
    private PersistentMap<String, Operation> acceptedOperations;

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
    private final ProtocolMessageHandler uponOperation = (protocolMessage) -> {
        OperationMessage message = (OperationMessage) protocolMessage;
        Operation operation = message.getOperation();
        this.acceptedOperations.put(KEY, operation);
        sendMessageToReplicaSet(new AcceptOperationMessage(++this.paxosInstance, operation));
    };
    private final ProtocolMessageHandler uponAcceptOperation = (protocolMessage) -> {
        AcceptOperationMessage message = (AcceptOperationMessage) protocolMessage;
        Operation operation = message.getOperation();
        int instance = message.getInstance();

        if (instance == this.paxosInstance + 1) {
            this.paxosInstance = instance;
            this.acceptedOperations.put(KEY, operation);

            ExecuteOperationRequest execOp = new ExecuteOperationRequest(message.getInstance(), operation);
            execOp.setDestination(PublishSubscribe.PROTOCOL_ID);
            deliverRequest(execOp);
        } else {


        }

    };
    private Map<Integer, Operation> pendingOperations;

    @Override
    public void init(Properties properties) {
        this.leader = null;
        this.leaderSN = 0;
        this.replicaSet = new HashSet<>();
        this.pendingOperations = new HashMap<>();
        this.mySequenceNumber = 0;
        this.acceptedOperations = new PersistentMap<>(myself.toString());
        //this.operationsToExecute = new LinkedList<>();
        this.prepareTimout = PropertiesUtils.getPropertyAsInt(properties, PREPARE_TIMEOUT);
        this.prepareOks = 0;
        this.prepareIssued = false;
        this.paxosInstance = 0;
    }

    private int prepareTimout;

    private void removeReplica(Host leader) {

    }

    private Operation pickHighestOperation(List<Operation> operations) {
        Map<Operation, Integer> result = new HashMap<>();
        int hSeq = Integer.MIN_VALUE;

        for (Operation op : operations) {
            Integer occ = result.get(op);
            if (occ == null) {
                occ = new Integer(0);
            }
            occ++;
            result.put(op, occ);
            if (occ > hSeq) {
                hSeq = occ;
            }
            if ((operations.size() / 2) + 1 <= occ) {
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

    private void sendPrepare() {
        this.prepareIssued = true;
        this.mySequenceNumber = getNextSequenceNumber();
        logger.info("[%s] sending prepare with sequence number %s\n", myself, this.mySequenceNumber);
        setupTimer(new PrepareTimer(this.mySequenceNumber), prepareTimout);
        this.prepareOks = 0;
        sendMessageToReplicaSet(new PrepareMessage(this.mySequenceNumber));
    }

    private int getNextSequenceNumber() {
        int seqNum = this.mySequenceNumber;
        while (seqNum < leaderSN) {
            seqNum += replicaSet.size();
        }

        return seqNum;
    }

    private void sendMessageToReplicaSet(ProtocolMessage message) {
        for (Host replica : replicaSet) {
            sendMessage(message, replica);
        }
    }

    private boolean hasMajority(int acks) {
        int replicaSize = this.replicaSet.size();
        if (replicaSize <= 2) {
            return acks == replicaSize;
        }

        return acks > (replicaSize / 2) + 1;
    }

    @Override
    public void nodeDown(Host host) {
        if (host.equals(leader) && amINextLeader()) {
            logger.info("[%s] becoming the new paxos leader\n", myself);
            sendPrepare();
        }
    }

    private boolean amINextLeader() {
        Set<Host> set = new HashSet<>(replicaSet);
        set.remove(leader);

        for (Host h : set) {
            if (myself.compareTo(h) < 0) {
                return false;
            }
        }

        return true;
    }

    @Override
    public void nodeUp(Host host) {
        // nothing to do here
    }

    @Override
    public void nodeConnectionReestablished(Host host) {
        // nothing to do here
    }

}