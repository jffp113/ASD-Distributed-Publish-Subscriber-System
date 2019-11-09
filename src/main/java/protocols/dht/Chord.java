package protocols.dht;

import babel.handlers.ProtocolMessageHandler;
import babel.handlers.ProtocolRequestHandler;
import babel.handlers.ProtocolTimerHandler;
import babel.protocol.GenericProtocol;
import network.Host;
import network.INetwork;
import network.INodeListener;
import protocols.dht.messages.*;
import protocols.dht.notifications.MessageDeliver;
import protocols.dht.requests.RouteRequest;
import protocols.dht.timers.FixFingersTimer;
import protocols.dht.timers.StabilizeTimer;
import protocols.floadbroadcastrecovery.requests.BCastRequest;
import protocols.partialmembership.timers.DebugTimer;
import utils.PropertiesUtils;

import java.net.InetAddress;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;


public class Chord extends GenericProtocol implements INodeListener {

    //final static Logger logger = LogManager.getLogger(Chord.class.getName());
    public static final short PROTOCOL_ID = 1243;
    public static final String PROTOCOL_NAME = "Chord";

    private static final String M = "m";
    private static final String CONTACT = "Contact";
    private static final String STABILIZE_INIT = "stabilizeInit";
    private static final String STABILIZE_PERIOD = "stabilizePeriod";
    private static final String FIX_FINGERS_INIT = "fixFingersInit";
    private static final String FIX_FINGERS_PERIOD = "fixFingersPeriod";
    private static final String DISPERSION_ALGORITHM = "SHA1";

    private int m; // Chord parameter, number of bits of dht ids.
    private int k; // Max Id
    private int myId;
    private Host predecessor;
    private Host successor;
    private List<FingerEntry> fingers;
    private int next; // Used to update finger
    private HashMap<String, Integer> referenceCounter;

    public Chord(INetwork net) throws Exception {
        super(PROTOCOL_NAME, PROTOCOL_ID, net);

        //logger.info("Building Chord");

        registerNodeListener(this);

        // Requests and replies
        registerNotification(MessageDeliver.NOTIFICATION_ID, MessageDeliver.NOTIFICATION_NAME);
        registerRequestHandler(BCastRequest.REQUEST_ID, uponRouteRequest);

        // Timers
        registerTimerHandler(StabilizeTimer.TimerCode, uponStabilizeTimer);
        registerTimerHandler(FixFingersTimer.TimerCode, uponFixFingersTimer);

        registerTimerHandler(DebugTimer.TimerCode, uponDebugTimer);

        // Messages
        registerMessageHandler(FindSuccessorRequestMessage.MSG_CODE,
                uponFindSuccessorRequestMessage, FindSuccessorRequestMessage.serializer);
        registerMessageHandler(FindSuccessorResponseMessage.MSG_CODE,
                uponFindSuccessorResponseMessage, FindSuccessorResponseMessage.serializer);
        registerMessageHandler(FindPredecessorRequestMessage.MSG_CODE,
                uponFindPredecessorRequestMessage, FindPredecessorRequestMessage.serializer);
        registerMessageHandler(FindPredecessorResponseMessage.MSG_CODE,
                uponFindPredecessorResponseMessage, FindPredecessorResponseMessage.serializer);
        registerMessageHandler(NotifyPredecessorMessage.MSG_CODE, uponNotifyPredecessorMessage,
                NotifyPredecessorMessage.serializer);
        registerMessageHandler(FindFingerSuccessorResponseMessage.MSG_CODE,
                uponFindFingerSuccessorResponseMessage, FindFingerSuccessorResponseMessage.serializer);
        registerMessageHandler(FindFingerSuccessorRequestMessage.MSG_CODE,
                uponFindFingerSuccessorRequestMessage, FindFingerSuccessorRequestMessage.serializer);

    }

    @Override
    public void init(Properties properties) {
        try {
            initStructures(properties);

            String contactString = PropertiesUtils.getPropertyAsString(properties, CONTACT);

            if (contactString != null) {
                String[] contactSplit = contactString.split(":");
                Host contact = new Host(InetAddress.getByName(contactSplit[0]), Integer.parseInt(contactSplit[1]));
                join(contact);
            }

            setupTimers(properties);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void setupTimers(Properties properties) {
        if (PropertiesUtils.getPropertyAsBool(properties, "debug")) {
            setupPeriodicTimer(new DebugTimer(), 1000, 10000);
        }

        setupPeriodicTimer(new StabilizeTimer(), PropertiesUtils.getPropertyAsInt(properties, STABILIZE_INIT),
                PropertiesUtils.getPropertyAsInt(properties, STABILIZE_PERIOD));
        setupPeriodicTimer(new FixFingersTimer(),
                PropertiesUtils.getPropertyAsInt(properties, FIX_FINGERS_INIT),
                PropertiesUtils.getPropertyAsInt(properties, FIX_FINGERS_PERIOD));
    }

    private void initStructures(Properties properties) {
        m = PropertiesUtils.getPropertyAsInt(properties, M);
        k = (int) Math.pow(2, m);
        myId = generateId();
        fingers = new ArrayList<>(m);
        next = 0;
        referenceCounter = new HashMap<>();
        createRing();
    }

    private final ProtocolTimerHandler uponDebugTimer = (protocolTimer) -> {
        System.out.println("--------------------");
        System.out.println(myself + "->" + myId);
        System.out.println("Predecessor: " + predecessor);
        System.out.println("Successor: " + successor);
        for (FingerEntry f : fingers) {
            System.out.println(f);
        }
    };

    private void join(Host node) {

        addNetworkPeer(node);
        sendMessage(new FindSuccessorRequestMessage(myId, myself), node);
    }

    private void createRing() {
        predecessor = null;

        for (int i = 0; i < m; i++) {
            int begin = calculateFinger(myId, i);
            fingers.add(i, new FingerEntry(begin, myId, myself));
        }

        changeSuccessor(myself);
    }

    //TODO: layer acima
    private final ProtocolRequestHandler uponRouteRequest = (protocolRequest) -> {
        RouteRequest request = (RouteRequest) protocolRequest;
    };

    private final ProtocolMessageHandler uponFindSuccessorRequestMessage = (protocolMessage) -> {
        FindSuccessorRequestMessage message = (FindSuccessorRequestMessage) protocolMessage;
        int nodeId = message.getNodeId();
        int successorId = calculateId(this.successor.toString());

        if (isIdBetween(nodeId, myId, successorId, true)) {
            sendMessageSideChannel(new FindSuccessorResponseMessage(successor, fingers), message.getRequesterNode());
        } else {
            Host closestPrecedingNode = closestPrecedingNode(nodeId);
            if (!closestPrecedingNode.equals(myself)) {
                sendMessage(message, closestPrecedingNode);
            } else {
                sendMessageSideChannel(new FindSuccessorResponseMessage(myself, fingers), message.getRequesterNode());
            }
        }
    };

    private final ProtocolMessageHandler uponFindSuccessorResponseMessage = (protocolMessage) -> {
        FindSuccessorResponseMessage message = (FindSuccessorResponseMessage) protocolMessage;
        changeSuccessor(message.getSuccessor());

        fillMyTable(message.getFingerEntryList());
    };

    private final ProtocolTimerHandler uponStabilizeTimer = (protocolTimer) -> {
        sendMessage(new FindPredecessorRequestMessage(), successor);
    };

    private final ProtocolMessageHandler uponFindPredecessorRequestMessage = (protocolMessage) -> {
        Host predecessorToSend = predecessor == null ? myself : predecessor;
        sendMessageSideChannel(new FindPredecessorResponseMessage(predecessorToSend), protocolMessage.getFrom());
    };

    private final ProtocolMessageHandler uponFindPredecessorResponseMessage = (protocolMessage) -> {
        FindPredecessorResponseMessage message = (FindPredecessorResponseMessage) protocolMessage;
        Host receivedPredecessor = message.getPredecessor();
        if (receivedPredecessor != null) {
            int tempId = calculateId(receivedPredecessor.toString());
            int successorId = calculateId(successor.toString());

            if (successorId == myId || isIdBetween(tempId, myId, successorId, false)) {
                changeSuccessor(receivedPredecessor);
            }
        }

        if (!successor.equals(myself)) {
            sendMessage(new NotifyPredecessorMessage(), successor);
        }
    };

    private final ProtocolMessageHandler uponNotifyPredecessorMessage = (protocolMessage) -> {
        Host sender = protocolMessage.getFrom();
        int senderId = calculateId(sender.toString());


        if (predecessor == null) {
            changePredecessor(sender);
        } else {
            int predecessorId = calculateId(predecessor.toString());

            if (isIdBetween(senderId, predecessorId, myId, false)) {
                removePredecessorNetworkPeer();
                changePredecessor(sender);
            }
        }
    };

    private final ProtocolTimerHandler uponFixFingersTimer = (protocolTimer) -> {
        if (++next == m) {
            next = 1;
        }

        FingerEntry finger = fingers.get(next);
        int successorToFindId = calculateFinger(myId, next);

        Host fingerHost = finger.getHost();
        if (!fingerHost.equals(myself)) {
            sendMessage(new FindFingerSuccessorRequestMessage(successorToFindId, myself, next), fingerHost);
        } else {
            sendMessage(new FindFingerSuccessorRequestMessage(successorToFindId, myself, next), getNewHostFromTable(myself));
        }
    };

    private final ProtocolMessageHandler uponFindFingerSuccessorRequestMessage = (protocolMessage) -> {
        FindFingerSuccessorRequestMessage message = (FindFingerSuccessorRequestMessage) protocolMessage;
        int nodeId = message.getNodeId();
        int successorId = calculateId(this.successor.toString());

        if (isIdBetween(nodeId, myId, successorId, true)) {
            sendMessageSideChannel(new FindFingerSuccessorResponseMessage(successor, message.getNext()), message.getRequesterNode());
        } else {
            Host closestPrecedingNode = closestPrecedingNode(nodeId);
            if (!closestPrecedingNode.equals(myself)) {
                sendMessage(message, closestPrecedingNode);
            }
        }
    };

    private final ProtocolMessageHandler uponFindFingerSuccessorResponseMessage = (protocolMessage) -> {
        FindFingerSuccessorResponseMessage message = (FindFingerSuccessorResponseMessage) protocolMessage;
        FingerEntry finger = fingers.get(message.getNext());

        Host successor = message.getSuccessor();

        updateFingerNetworkPeer(finger.getHost(), successor);

        finger.setHost(successor);
        finger.setHostId(calculateId(successor.toString()));
    };

    private void changePredecessor(Host predecessor) {
        addNetworkPeer(predecessor);
        this.predecessor = predecessor;

        // Verify integrity of the finger table
        for (FingerEntry finger : fingers) {
            if (finger.getHost().equals(myself)) {
                int predecessorID = calculateId(predecessor.toString());
                if (!isIdBetween(finger.getStart(), predecessorID, myId, true)) {
                    finger.setHost(successor);
                    finger.setHostId(fingers.get(0).getHostId());
                }
            }
        }
    }

    private void fillMyTable(List<FingerEntry> fingerEntryList) {
        for (int i = 1; i < m - 1; i++) {
            FingerEntry f = fingers.get(i);
            FingerEntry listSuccessor = findSuccessorInList(f.getStart(), fingerEntryList);
            if (f.getStart() - f.getHostId() > f.getStart() - listSuccessor.getHostId()) {
                updateFingerNetworkPeer(f.getHost(), listSuccessor.getHost());
                f.setHost(listSuccessor.getHost());
                f.setHostId(listSuccessor.getHostId());
            }
        }
    }

    private FingerEntry findSuccessorInList(int start, List<FingerEntry> fingerEntryList) {
        for (int i = 0; i < m; i++) {
            FingerEntry f = fingers.get(i);

            if (start <= f.getStart()) {
                return f;
            }
        }
        return fingerEntryList.get(0);
    }

    private Host closestPrecedingNode(int nodeId) {
        return closestPrecedingNode(nodeId, fingers, myself);
    }

    private Host closestPrecedingNode(int nodeId, List<FingerEntry> fingers, Host defaultHost) {
        FingerEntry finger;

        for (int i = m - 1; i >= 0; i--) {
            finger = fingers.get(i);
            if (isIdBetween(finger.getHostId(), calculateId(defaultHost.toString()), nodeId, false)) {
                return finger.getHost();
            }
        }

        return defaultHost;
    }

    private int generateId() {
        return calculateId(myself.toString());
    }

    private int calculateId(String seed) {
        try {
            String code = new String(MessageDigest.getInstance(DISPERSION_ALGORITHM).digest(seed.getBytes()));
            return Math.abs(code.hashCode() % k);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return 0;
        }
    }

    private int calculateFinger(int myId, int fingerIndex) {
        return (int) ((myId + Math.pow(2, fingerIndex)) % k);
    }

    private boolean isIdBetween(int id, int start, int end, boolean includeEnd) {
        int minLimit = start;
        int maxLimit = end;

        if (minLimit > maxLimit) {
            int amountToMaxLimit = Math.abs(k - id);
            if (amountToMaxLimit < id) {
                maxLimit = k;
            } else {
                minLimit = -1;
            }
        }

        return includeEnd ? start == end || id > minLimit && id <= maxLimit : id > minLimit && id < maxLimit;
    }

    private void changeSuccessor(Host newSuccessor) {
        int successorId = calculateId(newSuccessor.toString());

        FingerEntry finger = fingers.get(0);
        finger.setHost(newSuccessor);
        finger.setHostId(successorId);

        if (successor != null) {
            removeSuccessorNetworkPeer();
        }

        successor = newSuccessor;
        addNetworkPeer(successor);
    }

    @Override
    public void nodeDown(Host host) {
        if (predecessor.equals(host)) {
            removePredecessorNetworkPeer();
            predecessor = null;
        }

        if (successor.equals(host)) {
            changeSuccessor(myself);
        }

        for (FingerEntry finger : fingers) {
            Host fingerHost = finger.getHost();
            if (fingerHost.equals(host)) {
                Host newHost = getNewHostFromTable(host);
                updateFingerNetworkPeer(fingerHost, newHost);
                finger.setHost(newHost);
                finger.setHostId(calculateId(newHost.toString()));
            }
        }
    }

    private Host getNewHostFromTable(Host exclude) {
        for(FingerEntry entry : fingers) {
            if (!entry.getHost().equals(exclude) && !entry.getHost().equals(myself)) {
                return entry.getHost();
            }
        }
        return myself;
    }

    @Override
    public void nodeUp(Host host) {
    }

    @Override
    public void nodeConnectionReestablished(Host host) {
    }

    private void removeSuccessorNetworkPeer() {
        if (canRemovePeer(successor)) {
            removeNetworkPeer(successor);
        }
    }

    private void removePredecessorNetworkPeer() {
        if (canRemovePeer(predecessor)) {
            removeNetworkPeer(predecessor);
        }
    }

    private void updateFingerNetworkPeer(Host prev, Host next) {
        if (canRemovePeer(prev)) {
            removeNetworkPeer(prev);
        }

        addNetworkPeer(next);
    }

    private boolean canRemovePeer(Host peer) {
        if (peer.equals(predecessor) && peer.equals(successor)) {
            return false;
        } else {
            for (FingerEntry finger : fingers) {
                if (peer.equals(finger.getHost())) {
                    return false;
                }
            }
        }
        return true;
    }

}
