package protocols.publishsubscribe;

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
import protocols.publishsubscribe.messages.ForwardJoinMessage;
import protocols.publishsubscribe.messages.JoinMessage;

import java.util.Properties;
import java.util.Random;
import java.util.TreeSet;
import java.util.UUID;

public class HyParView extends GenericProtocol implements INodeListener {

    public static final short PROTO_ID = 1;
    public static final String ALG_NAME = "HyParView";
    private static final int LOG_N_PLUS_C = 10;
    private TreeSet<Host> activeView;
    private TreeSet<Host> passiveView;
    public int k, c, arwl, prwl;

    public HyParView(INetwork net) throws Exception {
        super(ALG_NAME, PROTO_ID, net);

        registerMessageHandler(JoinMessage.MSG_CODE, uponReceiveJoin, JoinMessage.serializer);
        registerMessageHandler(ForwardJoinMessage.MSG_CODE, uponReceiveForwardJoin, ForwardJoinMessage.serializer);

        registerRequestHandler(GetSampleRequest.REQUEST_ID, uponGetMembershipRequest);

    }

    @Override
    public void init(Properties properties) {
        this.activeView = new TreeSet<>();
        this.passiveView = new TreeSet<>();

        //TODO configurar o properties para ir buscar o contact
        addNodeToActiveView(null);
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
            GetSampleReply sampleReply = new GetSampleReply(UUID.randomUUID(), activeView);
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
