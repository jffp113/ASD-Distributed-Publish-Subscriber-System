package protocols.partialmembership;

import babel.exceptions.DestinationProtocolDoesNotExist;
import babel.handlers.ProtocolMessageHandler;
import babel.handlers.ProtocolRequestHandler;
import babel.protocol.GenericProtocol;
import babel.protocol.event.ProtocolMessage;
import babel.requestreply.ProtocolRequest;
import network.Host;
import network.INetwork;
import network.INodeListener;
import protocols.partialmembership.requests.GetSampleReply;
import protocols.partialmembership.requests.GetSampleRequest;
import protocols.partialmembership.messages.ForwardJoinMessage;
import protocols.partialmembership.messages.JoinMessage;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.Random;
import java.util.TreeSet;
import java.util.UUID;

public class HyParView extends GenericProtocol implements INodeListener {


    public static final short PROTOCOL_ID = 1;
    public static final String ALG_NAME = "HyParView";

    public static final String ACTIVE_RANDOM_WALK_LENGTH = "activeRandom";
    public static final String PASSIVE_RANDOM_WALK_LENGTH = "passiveRandom";
    public static final String PASSIVE_VIEW_CONSTANT = "passiveViewConstant";
    public static final String ACTIVE_VIEW_SIZE = "activeViewSize";

    private int LOG_N_PLUS_C = 10;
    private TreeSet<Host> activeView;
    private TreeSet<Host> passiveView;
    public int k,  arwl, prwl;

    public HyParView(INetwork net) throws Exception {
        super(ALG_NAME, PROTOCOL_ID, net);

        registerMessageHandler(JoinMessage.MSG_CODE, uponReceiveJoin, JoinMessage.serializer);
        registerMessageHandler(ForwardJoinMessage.MSG_CODE, uponReceiveForwardJoin, ForwardJoinMessage.serializer);
        registerRequestHandler(GetSampleRequest.REQUEST_ID, uponGetMembershipRequest);

    }

    @Override
    public void init(Properties properties) {
        this.activeView = new TreeSet<>();
        this.passiveView = new TreeSet<>();

        LOG_N_PLUS_C = Integer.parseInt(properties.getProperty(ACTIVE_VIEW_SIZE));
        k = Integer.parseInt(properties.getProperty(PASSIVE_VIEW_CONSTANT));
        arwl = Integer.parseInt(properties.getProperty(ACTIVE_RANDOM_WALK_LENGTH));
        prwl = Integer.parseInt(properties.getProperty(PASSIVE_RANDOM_WALK_LENGTH));
        String[] contact = properties.getProperty("Contact").split(":");
        Host host = null;
        try {
            host = new Host(InetAddress.getByName(contact[0]),Integer.parseInt(contact[1]));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        addNodeToActiveView(host);
    }


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
        Random r = new Random(System.currentTimeMillis());
        int index = r.nextInt() % LOG_N_PLUS_C;
        Host prevHost = null;
        for (Host h : activeView) {
            index--;
            if (index == 0) {
                if (h.compareTo(unwantedHost) != 0)
                    return h;
                else {
                    if (prevHost != null) {
                        return prevHost;
                    } else {
                        index++;
                    }
                }
            }
            prevHost = h;
        }
        return null;
    }

}
