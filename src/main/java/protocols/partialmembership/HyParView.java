package protocols.partialmembership;

import babel.exceptions.DestinationProtocolDoesNotExist;
import babel.handlers.ProtocolMessageHandler;
import babel.handlers.ProtocolRequestHandler;
import babel.handlers.ProtocolTimerHandler;
import babel.protocol.GenericProtocol;
import babel.protocol.event.ProtocolMessage;
import babel.requestreply.ProtocolRequest;
import babel.timer.ProtocolTimer;
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

    public static final String ACTIVE_RANDOM_WALK_LENGTH = "activeRandom";
    public static final String PASSIVE_RANDOM_WALK_LENGTH = "passiveRandom";
    public static final String PASSIVE_VIEW_CONSTANT = "passiveViewConstant";
    public static final String ACTIVE_VIEW_SIZE = "activeViewSize";
    public static final String SHUFFLE_INIT = "shuffleInit";
    public static final String SHUFFLE_PERIOD = "shufflePeriod";
    public static final String ELMS_TO_PICK_AV = "elmsToPickAV";
    public static final String ELMS_TO_PICK_PV = "elmsToPickPV";

    private int LOG_N_PLUS_C = 10;
    private TreeSet<Host> activeView;
    private TreeSet<Host> passiveView;
    private HashMap<UUID, ShuffleMessage> msgsSent;
    public int k, arwl, prwl, elmsToPickFromAV, elmsToPickFromPV;


    @Override
    public void init(Properties properties) {
        this.activeView = new TreeSet<>();
        this.passiveView = new TreeSet<>();
        this.msgsSent = new HashMap<>();

        long shuffleInit = Long.parseLong(properties.getProperty(SHUFFLE_INIT));
        long shufflePeriod = Long.parseLong(properties.getProperty(SHUFFLE_PERIOD));
        LOG_N_PLUS_C = Integer.parseInt(properties.getProperty(ACTIVE_VIEW_SIZE));
        k = Integer.parseInt(properties.getProperty(PASSIVE_VIEW_CONSTANT));
        arwl = Integer.parseInt(properties.getProperty(ACTIVE_RANDOM_WALK_LENGTH));
        prwl = Integer.parseInt(properties.getProperty(PASSIVE_RANDOM_WALK_LENGTH));
        elmsToPickFromAV = Integer.parseInt(properties.getProperty(ELMS_TO_PICK_AV));
        elmsToPickFromPV = Integers.parseInt(properties.getProperty(ELMS_TO_PICK_PV));
        String[] contact = properties.getProperty("Contact").split(":");

        setupPeriodicTimer(new ShuffleProtocolTimer(), shuffleInit, shufflePeriod);
        Host host = null;
        try {
            host = new Host(InetAddress.getByName(contact[0]), Integer.parseInt(contact[1]));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        addNodeToActiveView(host);
        sendMessage(new JoinMessage(myself), host);

    }

    private final ProtocolMessageHandler uponShuffle = new ProtocolMessageHandler() {
        @Override
        public void receive(ProtocolMessage protocolMessage) {
            ShuffleMessage shuffleMessage = (ShuffleMessage) protocolMessage;

            int ttl = shuffleMessage.decTtl();

            if (ttl > 0 && activeView.size() > 1) {
                Host h = selectRandomFromActiveView(shuffleMessage.getSender());
                sendMessage(shuffleMessage, h);
            } else {
                int elmsToPick = shuffleMessage.getPvSample().size() + shuffleMessage.getAvSample().size() + 1;
                Set<Host> sample = pickSampleFromSet(passiveView, elmsToPick);
                ShuffleReplyMessage replyMessage = new ShuffleReplyMessage(shuffleMessage.getMid(), sample);

                mergePassiveView(shuffleMessage.getAvSample(), shuffleMessage.getPvSample(), replyMessage.getNodes());

                sendMessage(replyMessage, shuffleMessage.getSender());
            }
        }
    };

    private final ProtocolTimerHandler uponShuffleTimer = new ProtocolTimerHandler() {
        @Override
        public void uponTimer(ProtocolTimer protocolTimer) {
            Set<Host> avSample = pickSampleFromSet(activeView, elmsToPickFromAV);
            Set<Host> pvSample = pickSampleFromSet(passiveView, elmsToPickFromPV);

            Host neighToShuffle = selectRandomFromActiveView(myself);

            ShuffleMessage msg = new ShuffleMessage(avSample, pvSample, myself, arwl);
            msgsSent.put(msg.getMid(), msg);
            sendMessage(msg, neighToShuffle);
        }
    };


    private final ProtocolMessageHandler uponReceiveJoin = new ProtocolMessageHandler() {
        @Override
        public void receive(ProtocolMessage msg) {
            JoinMessage joinMessage = (JoinMessage) msg;
            Host node = joinMessage.getNode();
            addNodeToActiveView(node);
            for (Host h : activeView) {
                if (h.compareTo(node) != 0) {
                    sendMessage(new ForwardJoinMessage(node, myself, arwl), h);
                }
            }
        }
    };

    private final ProtocolMessageHandler uponReceiveForwardJoin = new ProtocolMessageHandler() {
        @Override
        public void receive(ProtocolMessage msg) {
            ForwardJoinMessage forwardJoinMessage = (ForwardJoinMessage) msg;
            int ttl = forwardJoinMessage.getTtl();
            Host joinerHost = forwardJoinMessage.getJoinerHost();
            Host senderHost = forwardJoinMessage.getSenderHost();

            if (ttl == 0 || activeView.size() == 1) {
                addNodeToActiveView(joinerHost);
            } else {
                if (ttl == prwl) {
                    addNodeToPassiveView(joinerHost);
                }
                Host neigh = selectRandomFromActiveView(senderHost);
                if (neigh != null) {
                    sendMessage(new ForwardJoinMessage(joinerHost, myself, ttl - 1), neigh);
                }
            }
        }
    };

    private final ProtocolRequestHandler uponGetMembershipRequest = new ProtocolRequestHandler() {
        @Override
        public void uponRequest(ProtocolRequest protocolRequest) {
            // TODO depois ter atencao ao fanout

            GetSampleReply sampleReply = new GetSampleReply(((GetSampleRequest) protocolRequest).getIdentifier(),
                    activeView);
            sampleReply.invertDestination(protocolRequest);
            try {
                sendReply(sampleReply);
            } catch (DestinationProtocolDoesNotExist destinationProtocolDoesNotExist) {
                destinationProtocolDoesNotExist.printStackTrace();
            }
        }
    };
    private final ProtocolMessageHandler uponShuffleReply = new ProtocolMessageHandler() {
        @Override
        public void receive(ProtocolMessage protocolMessage) {
            ShuffleReplyMessage shuffleReplyMessage = (ShuffleReplyMessage) protocolMessage;
            ShuffleMessage shuffleMessage = msgsSent.remove(shuffleReplyMessage.getMid());

            Set<Host> sentNodes = new HashSet<>(shuffleMessage.getAvSample());
            sentNodes.addAll(shuffleMessage.getPvSample());
            mergePassiveView(shuffleReplyMessage.getNodes(), shuffleReplyMessage.getNodes(), sentNodes);
        }
    };

    public HyParView(INetwork net) throws Exception {
        super(ALG_NAME, PROTOCOL_ID, net);

        registerMessageHandler(JoinMessage.MSG_CODE, uponReceiveJoin, JoinMessage.serializer);
        registerMessageHandler(ForwardJoinMessage.MSG_CODE, uponReceiveForwardJoin, ForwardJoinMessage.serializer);
        registerRequestHandler(GetSampleRequest.REQUEST_ID, uponGetMembershipRequest);
        registerMessageHandler(ShuffleMessage.MSG_CODE, uponShuffle, ShuffleMessage.serializer);
        registerMessageHandler(ShuffleReplyMessage.MSG_CODE, uponShuffleReply, ShuffleReplyMessage.serializer);

        registerTimerHandler(ShuffleProtocolTimer.TIMERCODE, uponShuffleTimer);
    }

    private void addNodeToActiveView(Host node) {
        if (node.compareTo(myself) != 0 && !isNodeInActiveView(node)) {
            if (isFullActiveView()) {
                dropRandomNodeFromActiveView();
            }
            activeView.add(node);
            addNetworkPeer(node);
        }
    }

    private void addNodeToPassiveView(Host host) {
        if (host.compareTo(myself) != 0 && !isNodeInActiveView(host) && !isNodeInPassiveView(host)) {
            if (isFullPassiveView()) {
                dropRandomNodeFromPassiveView();
            }
            passiveView.add(host);
        }
    }

    private void dropRandomNodeFromPassiveView() {
        Random r = new Random(System.currentTimeMillis());
        int index = r.nextInt() % (k * LOG_N_PLUS_C);
        for (Host h : passiveView) {
            index--;
            if (index == 0) {
                passiveView.remove(h);
                break;
            }
        }
    }

    private boolean isFullPassiveView() {
        return passiveView.size() == (k * LOG_N_PLUS_C);
    }

    private void dropRandomNodeFromActiveView() {
        //FIXME
        Random r = new Random(System.currentTimeMillis());
        int index = r.nextInt() % LOG_N_PLUS_C;
        for (Host h : activeView) {
            index--;
            if (index == 0) {
                activeView.remove(h);
                removeNetworkPeer(h);
                passiveView.add(h);
                break;
            }
        }
    }

    private boolean isFullActiveView() {
        return activeView.size() == (LOG_N_PLUS_C - 1);
    }

    private boolean isNodeInActiveView(Host node) {
        return activeView.contains(node);
    }

    private boolean isNodeInPassiveView(Host node) {
        return passiveView.contains(node);
    }

    private Set<Host> pickSampleFromSet(Set<Host> setToPick, int elmsToPick) {
        List<Host> sample = new ArrayList<>(setToPick);

        Random r = new Random();

        while (sample.size() > elmsToPick) {
            sample.remove(r.nextInt(sample.size()));
        }

        return new HashSet<>(sample);
    }

    private void mergePassiveView(Set<Host> avToAdd, Set<Host> pvToAdd, Set<Host> sentNodes) {
        List<Host> av = new ArrayList<>(avToAdd);
        List<Host> pv = new ArrayList<>(pvToAdd);
        av.remove(myself);
        av.removeAll(activeView);
        av.removeAll(passiveView);
        pv.remove(myself);
        pv.removeAll(activeView);
        pv.removeAll(passiveView);

        int nodesToAdd = av.size() + pv.size();
        int maxPV = LOG_N_PLUS_C * k;

        // Remove sent nodes of passive view
        Iterator<Host> it = sentNodes.iterator();
        while (maxPV - passiveView.size() < nodesToAdd){
            if(!it.hasNext()) break;
            passiveView.remove(it.next());
        }

        while (maxPV - passiveView.size() < nodesToAdd) {
            dropRandomNodeFromPassiveView();
        }

        passiveView.addAll(av);
        passiveView.addAll(pv);
    }

    @Override
    public void nodeDown(Host host) {
        if (isNodeInActiveView(host)) {
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

    private Host selectRandomFromActiveView(Host unwantedHost) {
        List<Host> lotto = new ArrayList<>(this.activeView);
        Random r = new Random(System.currentTimeMillis());
        int index = Math.abs(r.nextInt() % Math.min(lotto.size(),LOG_N_PLUS_C));
        return lotto.get(index);
    }
}
