package protocols.partialmembership;

import babel.exceptions.DestinationProtocolDoesNotExist;
import babel.handlers.ProtocolMessageHandler;
import babel.handlers.ProtocolRequestHandler;
import babel.handlers.ProtocolTimerHandler;
import babel.protocol.GenericProtocol;
import network.Host;
import network.INetwork;
import network.INodeListener;
import org.apache.logging.log4j.core.util.Integers;
import protocols.partialmembership.messages.ForwardJoinMessage;
import protocols.partialmembership.messages.JoinMessage;
import protocols.partialmembership.messages.ShuffleMessage;
import protocols.partialmembership.messages.ShuffleReplyMessage;
import protocols.partialmembership.requests.GetSampleReply;
import protocols.partialmembership.requests.GetSampleRequest;
import protocols.partialmembership.timers.ShuffleProtocolTimer;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

public class HyParView extends GenericProtocol implements INodeListener {

    public static final short PROTOCOL_ID = 1;
    public static final String ALG_NAME = "HyParView";

    private static final String ACTIVE_RANDOM_WALK_LENGTH = "activeRandom";
    private static final String PASSIVE_RANDOM_WALK_LENGTH = "passiveRandom";
    private static final String PASSIVE_VIEW_CONSTANT = "passiveViewConstant";
    private static final String ACTIVE_VIEW_SIZE = "activeViewSize";
    private static final String SHUFFLE_INIT = "shuffleInit";
    private static final String SHUFFLE_PERIOD = "shufflePeriod";
    private static final String ELMS_TO_PICK_AV = "elmsToPickAV";
    private static final String ELMS_TO_PICK_PV = "elmsToPickPV";
    private static final String CONTACT = "Contact";

    private int LOG_N_PLUS_C = 10;
    private TreeSet<Host> activeView;
    private TreeSet<Host> passiveView;
    private HashMap<UUID, ShuffleMessage> shuffleMsgsSent;
    private int k, arwl, prwl, elmsToPickFromAV, elmsToPickFromPV;

    public HyParView(INetwork net) throws Exception {
        super(ALG_NAME, PROTOCOL_ID, net);

        registerRequestHandler(GetSampleRequest.REQUEST_ID, uponGetMembershipRequest);

        registerTimerHandler(ShuffleProtocolTimer.TIMERCODE, uponShuffleTimer);

        registerMessageHandler(JoinMessage.MSG_CODE, uponReceiveJoin, JoinMessage.serializer);
        registerMessageHandler(ForwardJoinMessage.MSG_CODE, uponReceiveForwardJoin, ForwardJoinMessage.serializer);
        registerMessageHandler(ShuffleMessage.MSG_CODE, uponShuffle, ShuffleMessage.serializer);
        registerMessageHandler(ShuffleReplyMessage.MSG_CODE, uponShuffleReply, ShuffleReplyMessage.serializer);

    }

    @Override
    public void init(Properties properties) {
        initProperties(properties);
        initStructures();

        long shuffleInit = Long.parseLong(properties.getProperty(SHUFFLE_INIT));
        long shufflePeriod = Long.parseLong(properties.getProperty(SHUFFLE_PERIOD));
        setupPeriodicTimer(new ShuffleProtocolTimer(), shuffleInit, shufflePeriod);

        String contact = properties.getProperty(CONTACT);
        startMembershipProtocol(contact);
    }

    @Override
    public void nodeDown(Host host) {
        if (!activeView.contains(host)) {
            activeView.remove(host);
            removeNetworkPeer(host);
            addNodeToPassiveView(host);
        }
    }

    @Override
    public void nodeUp(Host host) {
        addNodeToActiveView(host);
    }

    @Override
    public void nodeConnectionReestablished(Host host) {
        //TODO sendJoin??
    }

    private final ProtocolRequestHandler uponGetMembershipRequest = (protocolRequest) -> {
        try {
            GetSampleRequest req = (GetSampleRequest) protocolRequest;
            GetSampleReply sampleReply = new GetSampleReply(req.getIdentifier(), activeView);

            sampleReply.invertDestination(req);
            sendReply(sampleReply);
        } catch (DestinationProtocolDoesNotExist e) {
            //ignored - someone should exist waiting for the reply
            e.printStackTrace();
        }
    };

    private final ProtocolTimerHandler uponShuffleTimer = (protocolTimer) -> {
        Set<Host> avSample = pickSampleFromSet(activeView, elmsToPickFromAV);
        Set<Host> pvSample = pickSampleFromSet(passiveView, elmsToPickFromPV);

        Host neighToShuffle = selectRandomElementFromView(activeView);

        ShuffleMessage msg = new ShuffleMessage(avSample, pvSample, myself, arwl);

        shuffleMsgsSent.put(msg.getMid(), msg);
        sendMessage(msg, neighToShuffle);
    };

    private final ProtocolMessageHandler uponReceiveJoin = (protocolMessage) -> {
        JoinMessage joinMessage = (JoinMessage) protocolMessage;

        Host node = joinMessage.getNode();
        addNodeToActiveView(node);

        for (Host h : activeView) {
            if (!h.equals(node)) {
                sendMessage(new ForwardJoinMessage(node, myself, arwl), h);
            }
        }
    };

    private final ProtocolMessageHandler uponReceiveForwardJoin = (protocolMessage) -> {
        ForwardJoinMessage forwardJoinMessage = (ForwardJoinMessage) protocolMessage;

        int ttl = forwardJoinMessage.getTtl();
        Host newNode = forwardJoinMessage.getNewNode();
        Host sender = forwardJoinMessage.getSender();

        if (ttl == 0 || activeView.size() == 1) {
            addNodeToActiveView(newNode);
        } else {
            if (ttl == prwl) {
                addNodeToPassiveView(newNode);
            }
            Host neigh = selectRandomElementFromView(activeView, sender);
            if (neigh != null) {
                sendMessage(new ForwardJoinMessage(newNode, myself, ttl - 1), neigh);
            }
        }
    };

    private final ProtocolMessageHandler uponShuffle = (protocolMessage) -> {
        ShuffleMessage shuffleMessage = (ShuffleMessage) protocolMessage;

        int ttl = shuffleMessage.decTtl();

        if (ttl > 0 && activeView.size() > 1) {
            Host h = selectRandomElementFromView(activeView, shuffleMessage.getSender());
            sendMessage(shuffleMessage, h);
        } else {
            int elmsToPick = shuffleMessage.getPvSample().size() + shuffleMessage.getAvSample().size() + 1;
            Set<Host> sample = pickSampleFromSet(passiveView, elmsToPick);
            ShuffleReplyMessage replyMessage = new ShuffleReplyMessage(shuffleMessage.getMid(), sample);

            Set<Host> samplesMerged = mergeSets(shuffleMessage.getAvSample(), shuffleMessage.getPvSample());
            mergePassiveView(samplesMerged, replyMessage.getNodes());

            if (!activeView.contains(shuffleMessage.getSender())) {
                addNetworkPeer(shuffleMessage.getSender());
            }

            sendMessage(replyMessage, shuffleMessage.getSender());
        }
    };

    private final ProtocolMessageHandler uponShuffleReply = (protocolMessage) -> {
        ShuffleReplyMessage shuffleReplyMessage = (ShuffleReplyMessage) protocolMessage;
        ShuffleMessage shuffleMessage = shuffleMsgsSent.remove(shuffleReplyMessage.getMid());

        Set<Host> sentNodes = mergeSets(shuffleMessage.getAvSample(), shuffleMessage.getPvSample());
        mergePassiveView(shuffleReplyMessage.getNodes(), sentNodes);

        if (!activeView.contains(shuffleReplyMessage.getFrom())) {
            removeNetworkPeer(shuffleReplyMessage.getFrom());
        }
    };

    private void initProperties(Properties props) {
        LOG_N_PLUS_C = Integer.parseInt(props.getProperty(ACTIVE_VIEW_SIZE));
        k = Integer.parseInt(props.getProperty(PASSIVE_VIEW_CONSTANT));
        arwl = Integer.parseInt(props.getProperty(ACTIVE_RANDOM_WALK_LENGTH));
        prwl = Integer.parseInt(props.getProperty(PASSIVE_RANDOM_WALK_LENGTH));
        elmsToPickFromAV = Integer.parseInt(props.getProperty(ELMS_TO_PICK_AV));
        elmsToPickFromPV = Integers.parseInt(props.getProperty(ELMS_TO_PICK_PV));
    }

    private void initStructures() {
        this.activeView = new TreeSet<>();
        this.passiveView = new TreeSet<>();
        this.shuffleMsgsSent = new HashMap<>();
    }

    private void startMembershipProtocol(String contact) {
        try {
            String[] contactSplit = contact.split(":");
            String contactAddress = contactSplit[0];
            String contactPort = contactSplit[1];

            Host host = new Host(InetAddress.getByName(contactAddress), Integer.parseInt(contactPort));
            addNodeToActiveView(host);
            sendMessage(new JoinMessage(myself), host);
        } catch (UnknownHostException e) {
            // ignored - contact should exist
            e.printStackTrace();
        }
    }

    private void addNodeToActiveView(Host node) {
        if (!node.equals(myself) && !activeView.contains(node)) {
            if (isFullActiveView()) {
                dropRandomNodeFromActiveView();
            }
            activeView.add(node);
            addNetworkPeer(node);
        }
    }

    private void dropRandomNodeFromActiveView() {
        Host host = selectRandomElementFromView(activeView);

        activeView.remove(host);
        removeNetworkPeer(host);
        passiveView.add(host);
    }

    private void addNodeToPassiveView(Host host) {
        if (host.compareTo(myself) != 0 && !activeView.contains(host) && !passiveView.contains(host)) {
            if (isFullPassiveView()) {
                dropRandomNodeFromPassiveView();
            }
            passiveView.add(host);
        }
    }

    private void dropRandomNodeFromPassiveView() {
        Host host = selectRandomElementFromView(passiveView);

        passiveView.remove(host);
    }

    private boolean isFullPassiveView() {
        return passiveView.size() == (k * LOG_N_PLUS_C);
    }

    private boolean isFullActiveView() {
        return activeView.size() == (LOG_N_PLUS_C - 1);
    }

    private Set<Host> pickSampleFromSet(Set<Host> setToPick, int elmsToPick) {
        List<Host> sample = new ArrayList<>(setToPick);

        Random r = new Random();

        while (sample.size() > elmsToPick) {
            sample.remove(r.nextInt(sample.size()));
        }

        return new HashSet<>(sample);
    }

    private Host selectRandomElementFromView(Set<Host> view) {
        return selectRandomElementFromView(view, null);
    }

    private Host selectRandomElementFromView(Set<Host> view, Host unwantedElement) {
        List<Host> lotto = new ArrayList<>(view);
        if (unwantedElement != null) {
            lotto.remove(unwantedElement);
        }

        Random r = new Random(System.currentTimeMillis());
        int index = r.nextInt(Math.min(lotto.size(), LOG_N_PLUS_C));

        return lotto.get(index);
    }

    private void mergePassiveView(Set<Host> nodesToAdd, Set<Host> sentNodes) {
        List<Host> sampleToAdd = new ArrayList<>(nodesToAdd);
        sampleToAdd.remove(myself);
        sampleToAdd.removeAll(activeView);

        int nodesToAddSize = sampleToAdd.size();
        int maxPV = LOG_N_PLUS_C * k;

        // Remove sent nodes from passive view
        Iterator<Host> it = sentNodes.iterator();
        while (maxPV - passiveView.size() < nodesToAddSize) {
            if (!it.hasNext()) {
                break;
            }
            passiveView.remove(it.next());
        }

        // Remove random nodes from passive view
        while (maxPV - passiveView.size() < nodesToAddSize) {
            dropRandomNodeFromPassiveView();
        }

        passiveView.addAll(sampleToAdd);
    }

    private Set<Host> mergeSets(Set<Host> a, Set<Host> b) {
        Set<Host> set = new HashSet<>(a);
        set.addAll(b);
        return set;
    }
}
