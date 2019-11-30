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
import protocols.multipaxos.messages.*;
import protocols.multipaxos.notifications.DecideNotification;
import protocols.multipaxos.notifications.LeaderNotification;
import protocols.multipaxos.requests.ProposeRequest;
import protocols.multipaxos.timers.NoOpTimeout;
import protocols.multipaxos.timers.PrepareTimer;
import protocols.multipaxos.timers.SendNoOpTimer;
import protocols.partialmembership.timers.DebugTimer;
import protocols.publishsubscribe.requests.StartRequest;
import protocols.publishsubscribe.requests.StartRequestNotification;
import utils.PropertiesUtils;
import utils.Utils;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

public class MultiPaxos extends GenericProtocol implements INodeListener {
    public static final short PROTOCOL_ID = 15243;
    private static final String PROTOCOL_NAME = "MultiPaxos";

    private static final String PREPARE_TIMEOUT = "prepareTimeout";
    private static final String NO_OP_TIMER_INIT = "noOpTimerInit";
    private static final String NO_OP_TIMER_PERIOD = "noOpTimerPeriod";
    private static final String NO_OP_TIMEOUT = "noOpTimeout";

    private static final String MULTIPAXOS_CONTACT = "multipaxos_contact";
    private static final String STATIC_REPLICA_SIZE = "static_replica_size";

    static Logger logger = LogManager.getLogger(MultiPaxos.class.getName());

    private List<Host> replicas;
    private int mySequenceNumber;
    private Host leader;
    private int leaderSN;
    private int prepareTimout;
    private int prepareOks;
    private boolean prepareIssued;
    private int paxosInstance;
    private int maxReplicas;

    private final ProtocolTimerHandler uponSendNoOpTimer = (protocolTimer) -> {
        if (imLeader() && replicas.size() > 1) {
            AcceptOperationMessage message = new AcceptOperationMessage(new Operation(Utils.generateId(),
                    this.paxosInstance, this.mySequenceNumber, Operation.Type.NO_OP, new NoContent()));
            sendMessageToReplicas(message);
        }
    };

    private final ProtocolTimerHandler uponNoOpTimeout = (protocolTimer) -> {
        if(replicas.size() > 1)
            sendPrepare();
    };

    private final ProtocolRequestHandler uponStartRequest = (protocolRequest) -> {
        StartRequest request = (StartRequest) protocolRequest;
        Host contact = request.getContact();

        if (contact == null) {
            addNetworkPeer(myself);
            this.leader = myself;
            this.leaderSN = 1;
            this.mySequenceNumber = 1;
            this.replicas.add(myself);

            StartRequestNotification notification = new StartRequestNotification(this.replicas, this.leader, this.mySequenceNumber, this.paxosInstance);
            triggerNotification(notification);
        } else {
            AddReplicaMessage message = new AddReplicaMessage(myself);
            sendMessageSideChannel(message, contact);
        }
    };
    private final ProtocolMessageHandler uponAddReplicaMessage = (protocolMessage) -> {
        AddReplicaMessage message = (AddReplicaMessage) protocolMessage;
        if (imLeader()) {
            Content content = new MembershipUpdateContent(message.getReplica());
            Operation newReplicaOp = new Operation(Utils.generateId(), ++paxosInstance, this.mySequenceNumber, Operation.Type.ADD_REPLICA, content);
            sendMessageToReplicas(new AcceptOperationMessage(newReplicaOp));
            logger.info(String.format("Im leader processing request from %s\n", message.getReplica().toString()));
        } else {
            sendMessage(message, this.leader);
            logger.info(String.format("Forwarding message to leader %s\n", this.leader.toString()));
        }
    };
    private final ProtocolMessageHandler uponAddReplicaResponseMessage = (protocolMessage) -> {
        AddReplicaResponseMessage message = (AddReplicaResponseMessage) protocolMessage;

        this.leader = message.getFrom();
        this.leaderSN = message.getLeaderSN();
        this.replicas = message.getReplicas();

        for (Host replica : replicas) {
            addNetworkPeer(replica);
            if (!replicas.contains(replica)) {
                replicas.add(replica);
            }
        }

        this.paxosInstance = message.getInstance();
        this.mySequenceNumber = message.getReplicaSN();

        triggerNotification(new StartRequestNotification(replicas, leader, leaderSN, paxosInstance));
    };
    private final ProtocolRequestHandler uponProposeRequest = (protocolRequest) -> {
        ProposeRequest request = (ProposeRequest) protocolRequest;
        Operation operation = request.getOperation();
        accept(operation);
    };
    private Host oldLeader;
    private TreeSet<Operation> toAccept;
    private Map<Integer, Integer> operationOkAcks;
    private TreeSet<Operation> acceptedOperations;
    private final ProtocolMessageHandler uponPrepareMessage = (protocolMessage) -> {
        PrepareMessage message = (PrepareMessage) protocolMessage;

        if (this.leaderSN < message.getSequenceNumber()) {
            if (this.paxosInstance < message.getPaxosInstance()) {
                this.paxosInstance = message.getPaxosInstance();
            }

            this.leader = message.getFrom();
            this.leaderSN = message.getSequenceNumber();
            Set<Operation> missingOperations = getMissingOperations(message.getPaxosInstance());
            PrepareOk prepareOk = new PrepareOk(this.leaderSN, missingOperations);
            sendMessage(prepareOk, message.getFrom());
            deliverNotification(new LeaderNotification(this.leader, this.leaderSN));
        }
    };
    private NoOpTimeout noOpTimeout;
    private int nodesFailed;
    private final ProtocolMessageHandler uponAcceptOkOperation = (protocolMessage) -> {
        AcceptOkMessage message = (AcceptOkMessage) protocolMessage;
        Operation operation = message.getOperation();

        logger.info(String.format("[%s] ACCEPTOK TYPE:%s FROM:%s", myself, operation.getType(), message.getFrom()));

        if (operation.getSequenceNumber() == this.leaderSN) {
            // case when a new leader redecides an operation that was previously seen by this replica
            if (acceptedOperations.contains(operation)) {
                acceptedOperations.remove(operation);
                acceptedOperations.add(operation);
                return;
            }

            int oid = operation.getId();
            Integer acks = operationOkAcks.getOrDefault(oid, 0) + 1;
            operationOkAcks.put(oid, acks);

            if (hasMajority(acks)) {
                processOperationAccepted(operation);
            }

        }

    };
    private final ProtocolMessageHandler uponPrepareOk = (protocolMessage) -> {
        PrepareOk message = (PrepareOk) protocolMessage;

        if (this.mySequenceNumber == message.getSequenceNumber()) {
            prepareOks++;
            toAccept.addAll(message.getToAccept());

            if (hasMajority(prepareOks)) {
                this.leader = myself;
                this.leaderSN = this.mySequenceNumber;
                this.prepareOks = 0;
                this.prepareIssued = false;

                for (Operation op : toAccept) {
                    sendMessageToReplicas(new AcceptOperationMessage(new Operation(op.getId(), op.getInstance(),
                            this.mySequenceNumber, op.getType(), op.getContent())));
                }
                toAccept.clear();

                if (oldLeader != null) {
                    Content content = new MembershipUpdateContent(oldLeader);
                    Operation op = new Operation(Utils.generateId(), ++paxosInstance, this.mySequenceNumber, Operation.Type.REMOVE_REPLICA, content);
                    sendMessageToReplicas(new AcceptOperationMessage(op));
                }
            }
        }

    };
    private int noOpTimeoutPeriod;

    private final ProtocolTimerHandler uponDebugTimer = (protocolTimer) -> {
        StringBuilder sb = new StringBuilder();
        sb.append("--------------------\n");
        sb.append("Leader->" + leader + "(" + leaderSN + ")" + "\n");
        sb.append(myself + "(" + mySequenceNumber + ")" + "\n");
        sb.append(replicas + "\n");

        logger.info(sb.toString());
    };

    private final ProtocolMessageHandler uponAcceptOperation = (protocolMessage) -> {
        AcceptOperationMessage message = (AcceptOperationMessage) protocolMessage;
        Operation operation = message.getOperation();
        cancelTimer(this.noOpTimeout.getUuid());

        if (operation.getSequenceNumber() == this.leaderSN) {
            sendMessageToReplicas(new AcceptOkMessage(message.getOperation()));
        }

        this.noOpTimeout = new NoOpTimeout();
        setupTimer(noOpTimeout, noOpTimeoutPeriod);
    };

    public MultiPaxos(INetwork net) throws HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID, net);

        registerTimerHandler(DebugTimer.TimerCode, uponDebugTimer);
        registerTimerHandler(PrepareTimer.TIMER_CODE, uponPrepareTimer);
        registerTimerHandler(SendNoOpTimer.TIMER_CODE, uponSendNoOpTimer);
        registerTimerHandler(NoOpTimeout.TIMER_CODE, uponNoOpTimeout);

        registerNotification(DecideNotification.NOTIFICATION_ID, DecideNotification.NOTIFICATION_NAME);
        registerNotification(StartRequestNotification.NOTIFICATION_ID, StartRequestNotification.NOTIFICATION_NAME);
        registerNotification(LeaderNotification.NOTIFICATION_ID, LeaderNotification.NOTIFICATION_NAME);

        registerMessageHandler(AddReplicaMessage.MSG_CODE, uponAddReplicaMessage, AddReplicaMessage.serializer);
        registerMessageHandler(PrepareMessage.MSG_CODE, uponPrepareMessage, PrepareMessage.serializer);
        registerMessageHandler(PrepareOk.MSG_CODE, uponPrepareOk, PrepareOk.serializer);
        registerMessageHandler(AcceptOperationMessage.MSG_CODE, uponAcceptOperation, AcceptOperationMessage.serializer);
        registerMessageHandler(AcceptOkMessage.MSG_CODE, uponAcceptOkOperation, AcceptOkMessage.serializer);
        registerMessageHandler(AddReplicaResponseMessage.MSG_CODE, uponAddReplicaResponseMessage, AddReplicaResponseMessage.serializer);

        registerRequestHandler(StartRequest.REQUEST_ID, uponStartRequest);
        registerRequestHandler(ProposeRequest.REQUEST_ID, uponProposeRequest);
        registerNodeListener(this);
    }

    @Override
    public void init(Properties properties) {
        this.replicas = new LinkedList<>();
        this.operationOkAcks = new HashMap<>();
        this.prepareTimout = PropertiesUtils.getPropertyAsInt(properties, PREPARE_TIMEOUT);
        this.noOpTimeoutPeriod = PropertiesUtils.getPropertyAsInt(properties, NO_OP_TIMEOUT);
        this.prepareIssued = false;
        this.toAccept = new TreeSet<>();
        this.acceptedOperations = new TreeSet<>();
        this.noOpTimeout = new NoOpTimeout();
        this.maxReplicas = PropertiesUtils.getPropertyAsInt(properties, STATIC_REPLICA_SIZE);

        setupPeriodicTimer(new DebugTimer(), 1000, 10000);
        setupPeriodicTimer(new SendNoOpTimer(), PropertiesUtils.getPropertyAsInt(properties, NO_OP_TIMER_INIT),
                PropertiesUtils.getPropertyAsInt(properties, NO_OP_TIMER_PERIOD));

       // initMultiPaxos(properties);
    }

    private void initMultiPaxos(Properties properties) {
        StartRequest request = new StartRequest();
        request.setDestination(MultiPaxos.PROTOCOL_ID);
        String rawContacts = PropertiesUtils.getPropertyAsString(properties, MULTIPAXOS_CONTACT);

        if (rawContacts != null) {
            String[] multipaxosContact = rawContacts.split(":");
            Host host = getHost(multipaxosContact);
            sendMessageSideChannel(new AddReplicaMessage(myself), host);
        } else {
            addNetworkPeer(myself);
            this.leader = myself;
            this.leaderSN = 1;
            this.mySequenceNumber = 1;
            this.replicas.add(myself);

           /* StartRequestNotification notification = new StartRequestNotification(this.replicas, this.leader, this.mySequenceNumber, this.paxosInstance);
            triggerNotification(notification);*/
        }

    }

    private Host getHost(String[] contact) {
        try {
            return new Host(InetAddress.getByName(contact[0]), Integer.parseInt(contact[1]));
        } catch (UnknownHostException e) {
            // Ignored
            e.printStackTrace();
        }
        return null;
    }

    private Set<Operation> getMissingOperations(int paxosInstance) {
        Set<Operation> missing = new HashSet<>();
        for (Operation op : acceptedOperations) {
            if (op.getInstance() >= paxosInstance) {
                missing.add(op);
            }
        }

        return missing;
    }

    private void processOperationAccepted(Operation operation) {
        if (operation.getInstance() > this.paxosInstance) {
            this.paxosInstance = operation.getInstance();
        }

        acceptedOperations.add(operation);
        operationOkAcks.remove(operation.getId());

        switch (operation.getType()) {
            case NO_OP:
                return;
            case ADD_REPLICA:
                logger.info(String.format("CONTENT:%s", ((MembershipUpdateContent) operation.getContent()).getReplica()));
                processAddReplica(operation);
                break;
            case REMOVE_REPLICA:
                logger.info(String.format("CONTENT:%s", ((MembershipUpdateContent) operation.getContent()).getReplica()));
                processRemoveReplica(operation);
                break;
        }

        triggerNotification(new DecideNotification(operation));
    }

    private void sendMessageToReplicas(ProtocolMessage message) {
        for (Host replica : replicas) {
            sendMessage(message, replica);
        }
    }

    private void processAddReplica(Operation operation) {
        MembershipUpdateContent content = (MembershipUpdateContent) operation.getContent();

        if (!replicas.contains(content.getReplica())) {
            replicas.add(content.getReplica());
            addNetworkPeer(content.getReplica());
        }

        if (imLeader()) {
            sendMessage(new AddReplicaResponseMessage(replicas, Utils.generateSeqNum(), this.mySequenceNumber, paxosInstance), content.getReplica());
        }
    }

    private void processRemoveReplica(Operation operation) {
        MembershipUpdateContent content = (MembershipUpdateContent) operation.getContent();

        if (content.getReplica().equals(oldLeader)) {
            this.oldLeader = null;
        }

        replicas.remove(content.getReplica());
        this.nodesFailed--;
        removeNetworkPeer(content.getReplica());
    }

    private void accept(Operation operation) {
        Operation op = new Operation(operation.getId(), ++paxosInstance, this.mySequenceNumber, operation.getType(), operation.getContent());
        sendMessageToReplicas(new AcceptOperationMessage(op));
    }

    private final ProtocolTimerHandler uponPrepareTimer = (protocolTimer) -> {
        if (prepareIssued) {
            sendPrepare();
        }
    };

    private boolean imLeader() {
        return myself.equals(this.leader);
    }

    private void sendPrepare() {
        this.prepareIssued = true;
        this.mySequenceNumber = getNextSequenceNumber();
        logger.info("[%s] sending prepare with sequence number %s\n", myself, this.mySequenceNumber);
        setupTimer(new PrepareTimer(this.mySequenceNumber), prepareTimout);
        this.prepareOks = 0;
        sendMessageToReplicas(new PrepareMessage(this.mySequenceNumber, ++this.paxosInstance));
    }

    private int getNextSequenceNumber() {
        int seqNum = this.mySequenceNumber + this.maxReplicas;
        while (seqNum < leaderSN) {
            seqNum += this.maxReplicas;
        }

        return seqNum;
    }

    private boolean hasMajority(int acks) {
        int replicaSize = this.replicas.size() - this.nodesFailed;
        if (replicaSize <= 2) {
            return acks == replicaSize;
        }
        return acks >= (replicaSize / 2) + 1;
    }

    @Override
    public void nodeDown(Host host) {
        this.nodesFailed++;

        if (imLeader()) {
            Content content = new MembershipUpdateContent(host);
            Operation op = new Operation(Utils.generateId(), ++paxosInstance, this.mySequenceNumber, Operation.Type.REMOVE_REPLICA, content);
            sendMessageToReplicas(new AcceptOperationMessage(op));
        }

        if (host.equals(leader)) {
            logger.info("[%s] becoming the new paxos leader\n", myself);
            this.oldLeader = this.leader;
            sendPrepare();
        }
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
