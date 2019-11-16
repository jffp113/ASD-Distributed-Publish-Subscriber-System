package protocols.dht;

import babel.exceptions.DestinationProtocolDoesNotExist;
import babel.handlers.ProtocolMessageHandler;
import babel.handlers.ProtocolRequestHandler;
import babel.handlers.ProtocolTimerHandler;
import babel.protocol.GenericProtocol;
import network.Host;
import network.INetwork;
import network.INodeListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.dht.messages.*;
import protocols.dht.requests.RouteRequest;
import protocols.dht.timers.FixFingersTimer;
import protocols.dht.timers.StabilizeTimer;
import protocols.dissemination.Scribe;
import protocols.dissemination.notifications.RouteDelivery;
import protocols.dissemination.requests.RouteOk;
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

    final static Logger logger = LogManager.getLogger(Chord.class.getName());
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
        logger.info("Building Chord");

        registerNodeListener(this);

        // Requests and replies
        registerNotification(RouteDelivery.NOTIFICATION_ID, RouteDelivery.NOTIFICATION_NAME);
        registerRequestHandler(RouteRequest.REQUEST_ID, uponRouteRequest);

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
        registerMessageHandler(ForwardMessage.MSG_CODE, uponForwardMessage, ForwardMessage.serializer);
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

    private final ProtocolRequestHandler uponRouteRequest = (protocolRequest) -> {
        RouteRequest request = (RouteRequest) protocolRequest;
        logger.info(String.format("Process [%d]%s Routing %s with Id=%d", myId, myself, request.getMessageToRoute(), calculateId(request.getTopic())));
        int topicId = calculateId(request.getTopic());
        System.err.println(request.getMessageToRoute().getMessageType()+"from "+request.getMessageToRoute().getHost());
        if (isSuccessor(topicId)) {
            triggerNotification(new RouteDelivery(request.getMessageToRoute()));
            logger.info(String.format("[%d]%s RouteOk Message: %s", myId, myself, request.getMessageToRoute()));
        } else {
            Host host = closestPrecedingNode(calculateId(request.getTopic()));

            try {
                RouteOk routeOk = new RouteOk(request.getTopic(), host);
                routeOk.setDestination(Scribe.PROTOCOL_ID);
                sendReply(routeOk);
            } catch (DestinationProtocolDoesNotExist destinationProtocolDoesNotExist) {
                destinationProtocolDoesNotExist.printStackTrace();
            }

            sendMessage(new ForwardMessage(request.getMessageToRoute()), host);
            logger.info(String.format("[%d]%s Sending To %s Message: %s", myId, myself, host, request.getMessageToRoute()));
        }
    };

    private final ProtocolTimerHandler uponDebugTimer = (protocolTimer) -> {
       /* StringBuilder sb = new StringBuilder();
        sb.append("--------------------\n");
        sb.append(myself + "->" + myId + "\n");
        sb.append("Predecessor: " + predecessor + " " + (predecessor == null ? -1 : calculateId(predecessor.toString()) + "\n"));
        sb.append("Successor: " + successor + " " + calculateId(successor.toString()) + "\n");
        for (FingerEntry f : fingers) {
            sb.append(f + "\n");
        }

        logger.info(sb.toString());*/
    };

    private void join(Host node) {
        sendMessageSideChannel(new FindSuccessorRequestMessage(myId, myself), node);
    }

    private void createRing() {
        predecessor = null;

        for (int i = 0; i < m; i++) {
            int begin = calculateFinger(myId, i);
            fingers.add(i, new FingerEntry(begin, myId, myself));
        }

        changeSuccessor(myself);
    }

    private final ProtocolMessageHandler uponFindSuccessorRequestMessage = (protocolMessage) -> {
        FindSuccessorRequestMessage message = (FindSuccessorRequestMessage) protocolMessage;
        int nodeId = message.getNodeId();

        if (isSuccessor(nodeId)) {
            sendMessageSideChannel(new FindSuccessorResponseMessage(myself, fingers), message.getRequesterNode());
        } else {
            Host closestPrecedingNode = closestPrecedingNode(nodeId);
            if (!closestPrecedingNode.equals(myself)) {
                sendMessage(message, closestPrecedingNode);
            } else {
                sendMessageSideChannel(new FindSuccessorResponseMessage(myself, fingers), message.getRequesterNode());
            }
        }
    };
    private final ProtocolMessageHandler uponFindPredecessorResponseMessage = (protocolMessage) -> {
        FindPredecessorResponseMessage message = (FindPredecessorResponseMessage) protocolMessage;
        Host receivedPredecessor = message.getPredecessor();

        int receivedPredecessorId = calculateId(receivedPredecessor.toString());
        int successorId = fingers.get(0).getHostId();

        if (successorId == myId || isIdBetween(receivedPredecessorId, myId, successorId)) {
            changeSuccessor(receivedPredecessor);
        }

        if (!successor.equals(myself)) {
            sendMessage(new NotifyPredecessorMessage(), successor);
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
    private final ProtocolMessageHandler uponNotifyPredecessorMessage = (protocolMessage) -> {
        Host sender = protocolMessage.getFrom();
        int senderId = calculateId(sender.toString());

        if (isSuccessor(senderId)) {
            changePredecessor(sender);
        }
    };
    private final ProtocolTimerHandler uponFixFingersTimer = (protocolTimer) -> {
        if (++next == m) {
            next = 1;
        }

        FingerEntry finger = fingers.get(next);

        Host fingerHost = finger.getHost();
        if (!fingerHost.equals(myself)) {
            sendMessage(new FindFingerSuccessorRequestMessage(finger.getStart(), myself, next), fingerHost);
        } else {
            Host aux = getNewHostFromTable(myself);
            if (!aux.equals(myself)) {
                sendMessage(new FindFingerSuccessorRequestMessage(finger.getStart(), myself, next), aux);
            }
        }
    };
    private final ProtocolMessageHandler uponFindFingerSuccessorRequestMessage = (protocolMessage) -> {
        FindFingerSuccessorRequestMessage message = (FindFingerSuccessorRequestMessage) protocolMessage;
        int nodeId = message.getNodeId();
        if (predecessor != null && isIdBetween(nodeId, calculateId(predecessor.toString()), myId)) {
            sendMessageSideChannel(new FindFingerSuccessorResponseMessage(myself, message.getNext()), message.getRequesterNode());
        } else {
            Host closestPrecedingNode = closestPrecedingNode(nodeId);
            if (!closestPrecedingNode.equals(myself)) {
                sendMessage(message, closestPrecedingNode);
            }
        }

    };

    private void initStructures(Properties properties) {
        m = PropertiesUtils.getPropertyAsInt(properties, M);
        k = (int) Math.pow(2, m);
        myId = calculateId(myself.toString());
        fingers = new ArrayList<>(m);
        next = 0;
        referenceCounter = new HashMap<>();
        createRing();
    }

    private final ProtocolMessageHandler uponFindFingerSuccessorResponseMessage = (protocolMessage) -> {
        FindFingerSuccessorResponseMessage message = (FindFingerSuccessorResponseMessage) protocolMessage;
        FingerEntry finger = fingers.get(message.getNext());

        Host successor = message.getSuccessor();

        updateFingerNetworkPeer(finger.getHost(), successor);

        finger.setHost(successor);
        finger.setHostId(calculateId(successor.toString()));
    };

    private void changePredecessor(Host newPredecessor) {
        if (predecessor != null) {
            removePredecessorNetworkPeer();
        }
        addNetworkPeer(newPredecessor);
        predecessor = newPredecessor;
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
        FingerEntry finger;

        for (int i = 1; i < m; i++) {
            finger = fingers.get(i);
            if (isIdBetween(nodeId, fingers.get(i - 1).getStart(), finger.getStart())) {
                return fingers.get(i - 1).getHost();
            }
        }

        return fingers.get(0).getHost();
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

    private boolean isIdBetween(int id, int start, int end) {
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

        return id == end || id > minLimit && id <= maxLimit;
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
        verify();
    }

    @Override
    public void nodeDown(Host host) {
        if (host.equals(predecessor)) {
            removePredecessorNetworkPeer();
            predecessor = null;
        }

        if (successor.equals(host)) {
            changeSuccessor(myself);
            Host aux = getNewHostFromTable(host);
            if (!myself.equals(aux))
                sendMessage(new FindSuccessorRequestMessage(myId, myself), aux);
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
        for (FingerEntry entry : fingers) {
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

    private final ProtocolMessageHandler uponForwardMessage = (message) -> {
        RouteDelivery routeDelivery = new RouteDelivery(((ForwardMessage) message).getScribeMessage());

        triggerNotification(routeDelivery);
    };

    private boolean isSuccessor(int candidate) {
        return predecessor == null || isIdBetween(candidate, calculateId(predecessor.toString()), myId);
    }

    private void verify() {
        int successorId = calculateId(successor.toString());
        for (FingerEntry e : fingers) {
            if (isIdBetween(successorId, e.getStart(), e.getHostId())) {
                e.setHostId(successorId);
                e.setHost(successor);
            }
        }
    }
}
