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
import protocols.partialmembership.timers.DebugTimer;
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

    private Set<Host> activeView;
    private Set<Host> passiveView;
    private HashMap<UUID, ShuffleMessage> shuffleMessagesSent;
    private int logNPlusC, k, arwl, prwl, elmsToPickFromAV, elmsToPickFromPV;

    public HyParView(INetwork net) throws Exception {
        super(ALG_NAME, PROTOCOL_ID, net);

        registerNodeListener(this);

        registerRequestHandler(GetSampleRequest.REQUEST_ID, uponGetMembershipRequest);

        registerTimerHandler(ShuffleProtocolTimer.TIMERCODE, uponShuffleTimer);
        registerTimerHandler(DebugTimer.TimerCode, uponDebugTimer);

        registerMessageHandler(JoinMessage.MSG_CODE, uponReceiveJoin, JoinMessage.serializer);
        registerMessageHandler(ForwardJoinMessage.MSG_CODE, uponReceiveForwardJoin, ForwardJoinMessage.serializer);
        registerMessageHandler(ShuffleMessage.MSG_CODE, uponShuffle, ShuffleMessage.serializer);
        registerMessageHandler(ShuffleReplyMessage.MSG_CODE, uponShuffleReply, ShuffleReplyMessage.serializer);
    }

    private final ProtocolTimerHandler uponDebugTimer = (protocolTimer) -> {
        System.out.println("Active");
        System.out.println(activeView);
        System.out.println("passive");
        System.out.println(passiveView);
    };

    @Override
    public void init(Properties properties) {
        initStructures();
        initProperties(properties);

        long shuffleInit = Long.parseLong(properties.getProperty(SHUFFLE_INIT));
        long shufflePeriod = Long.parseLong(properties.getProperty(SHUFFLE_PERIOD));
        setupPeriodicTimer(new ShuffleProtocolTimer(), shuffleInit, shufflePeriod);
        setupPeriodicTimer(new DebugTimer(), 2000, 5000);

        String contact = properties.getProperty(CONTACT);

        joinProtocol(contact);
    }

    @Override
    public void nodeDown(Host host) {
        if (activeView.contains(host)) {
            activeView.remove(host);
            removeNetworkPeer(host);
            addNodeToPassiveView(host);
            Host toPromote = selectRandomFromView(passiveView,host);
            addNodeToActiveView(toPromote);
        }
    }

    @Override
    public void nodeUp(Host host) { //TODO SEE
        addNodeToActiveView(host);
        passiveView.remove(host);
    }

    @Override
    public void nodeConnectionReestablished(Host host) {
        // Nothing to do here.
    }

    private final ProtocolRequestHandler uponGetMembershipRequest = (protocolRequest) -> {
        try {
            GetSampleRequest sampleRequest = (GetSampleRequest) protocolRequest;
            int fanout = sampleRequest.getFanout();

            GetSampleReply sampleReply = new GetSampleReply(sampleRequest.getIdentifier(), pickSampleFromSet(activeView,fanout));

            sampleReply.invertDestination(protocolRequest);
            sendReply(sampleReply);
        } catch (DestinationProtocolDoesNotExist e) {
            // Ignored - destination should exist.
            e.printStackTrace();
        }

    };

    private final ProtocolTimerHandler uponShuffleTimer = (protocolTimer) -> {
        if (!activeView.isEmpty()) {
            Set<Host> avSample = pickSampleFromSet(activeView, elmsToPickFromAV);
            Set<Host> pvSample = pickSampleFromSet(passiveView, elmsToPickFromPV);

            Host neighToShuffle = selectRandomFromView(activeView);

            ShuffleMessage shuffleMessage = new ShuffleMessage(avSample, pvSample, myself, arwl);
            shuffleMessagesSent.put(shuffleMessage.getMid(), shuffleMessage);
            sendMessage(shuffleMessage, neighToShuffle);
        }

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
            Host neigh = selectRandomFromView(activeView, sender);
            if (neigh != null) {
                sendMessage(new ForwardJoinMessage(newNode, myself, ttl - 1), neigh);
            }
        }
    };

    private final ProtocolMessageHandler uponShuffle = (protocolMessage) -> {
        ShuffleMessage shuffleMessage = (ShuffleMessage) protocolMessage;

        int ttl = shuffleMessage.decTtl();

        if (ttl > 0 && activeView.size() > 1) {
            Host neigh = selectRandomFromView(activeView, shuffleMessage.getSender());
            sendMessage(shuffleMessage, neigh);
        } else {
            int elmsToPick = shuffleMessage.getPvSample().size() + shuffleMessage.getAvSample().size() + 1;
            Set<Host> sample = pickSampleFromSet(passiveView, elmsToPick);

            ShuffleReplyMessage replyMessage = new ShuffleReplyMessage(shuffleMessage.getMid(), sample);

            Set<Host> samples = mergeSamples(shuffleMessage.getAvSample(), shuffleMessage.getPvSample());

            mergeIntoPassiveView(samples, replyMessage.getNodes());

            if (activeView.contains(shuffleMessage.getSender())) {//TODO
                sendMessage(replyMessage, shuffleMessage.getSender());
            }
            else {
                sendMessageSideChannel(replyMessage, shuffleMessage.getSender());
            }

        }

    };

    private final ProtocolMessageHandler uponShuffleReply = (protocolMessage) -> {
        ShuffleReplyMessage shuffleReplyMessage = (ShuffleReplyMessage) protocolMessage;
        ShuffleMessage shuffleMessage = shuffleMessagesSent.remove(shuffleReplyMessage.getMid());

        Set<Host> sentNodes = mergeSamples(shuffleMessage.getAvSample(), shuffleMessage.getPvSample());
        mergeIntoPassiveView(shuffleReplyMessage.getNodes(), sentNodes);

    };

    private void initProperties(Properties properties) {
        logNPlusC = Integer.parseInt(properties.getProperty(ACTIVE_VIEW_SIZE));
        k = Integer.parseInt(properties.getProperty(PASSIVE_VIEW_CONSTANT));
        arwl = Integer.parseInt(properties.getProperty(ACTIVE_RANDOM_WALK_LENGTH));
        prwl = Integer.parseInt(properties.getProperty(PASSIVE_RANDOM_WALK_LENGTH));
        elmsToPickFromAV = Integer.parseInt(properties.getProperty(ELMS_TO_PICK_AV));
        elmsToPickFromPV = Integers.parseInt(properties.getProperty(ELMS_TO_PICK_PV));
    }

    private void initStructures() {
        this.activeView = Collections.synchronizedSet(new TreeSet<>());
        this.passiveView = Collections.synchronizedSet(new TreeSet<>());
        this.shuffleMessagesSent = new HashMap<>();
    }

    private void joinProtocol(String contact) {
        try {
            String[] contactSplit = contact.split(":");
            Host host = new Host(InetAddress.getByName(contactSplit[0]), Integer.parseInt(contactSplit[1]));
            addNodeToActiveView(host);
            sendMessage(new JoinMessage(myself), host);
        } catch (UnknownHostException e) {
            // Ignored - contact should exist
            e.printStackTrace();
        }
    }

    private Set<Host> mergeSamples(Set<Host> sample1, Set<Host> sample2) {
        Set<Host> samples = new HashSet<>(sample1);
        samples.addAll(sample2);
        return samples;
    }

    private void addNodeToActiveView(Host node) {
        if (!node.equals(myself) && !activeView.contains(node)) {
            if (isFullActiveView()) {
                dropRandomNodeFromActiveView();
            }

            if(passiveView.contains(node)){
                passiveView.remove(node);
            }

            activeView.add(node);
            addNetworkPeer(node);
        }
    }

    private void addNodeToPassiveView(Host node) {
        if (!node.equals(myself) && !activeView.contains(node) && !passiveView.contains(node)) {
            if (isFullPassiveView()) {
                dropRandomNodeFromPassiveView();
            }
            passiveView.add(node);
        }
    }

    private void dropRandomNodeFromPassiveView() {
        Host toDrop = selectRandomFromView(passiveView);
        passiveView.remove(toDrop);
    }

    private boolean isFullPassiveView() {
        return passiveView.size() == (k * logNPlusC);
    }

    private void dropRandomNodeFromActiveView() {
        Host toDrop = selectRandomFromView(activeView);
        activeView.remove(toDrop);
        removeNetworkPeer(toDrop);
        passiveView.add(toDrop);
    }

    private Host selectRandomFromView(Set<Host> view) {
        return selectRandomFromView(view, null);
    }

    private Host selectRandomFromView(Set<Host> view, Host notToPick) {
        List<Host> lotto = new ArrayList<>(view);

        if (notToPick != null) {
            lotto.remove(notToPick);
        }

        Random r = new Random(System.currentTimeMillis());
        int index = r.nextInt(lotto.size());
        return lotto.get(index);
    }

    private boolean isFullActiveView() {
        return activeView.size() == (logNPlusC);
    }

    private Set<Host> pickSampleFromSet(Set<Host> setToPick, int elmsToPick) {
        List<Host> sample = new ArrayList<>(setToPick);

        Random r = new Random();

        while (sample.size() > elmsToPick) {
            sample.remove(r.nextInt(sample.size()));
        }

        return new HashSet<>(sample);
    }

    private void mergeIntoPassiveView(Set<Host> samples, Set<Host> sentNodes) {
        List<Host> samplesToAdd = new ArrayList<>(samples);

        samplesToAdd.remove(myself);
        samplesToAdd.removeAll(activeView);
        samplesToAdd.removeAll(passiveView);

        int nodesToAdd = samplesToAdd.size();

        int maxPVSize = k * logNPlusC;
        Iterator<Host> it = sentNodes.iterator();

        while (maxPVSize - passiveView.size() < nodesToAdd) {
            if (!it.hasNext()) break;
            passiveView.remove(it.next());
        }

        while (maxPVSize - passiveView.size() < nodesToAdd) {
            dropRandomNodeFromPassiveView();
        }

        passiveView.addAll(samplesToAdd);
    }

}
