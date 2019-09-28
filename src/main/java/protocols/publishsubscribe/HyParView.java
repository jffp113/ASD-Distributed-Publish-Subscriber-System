package protocols.publishsubscribe;

import babel.handlers.ProtocolMessageHandler;
import babel.protocol.GenericProtocol;
import babel.protocol.event.ProtocolMessage;
import network.Host;
import network.INetwork;
import network.INodeListener;
import protocols.publishsubscribe.messages.JoinMessage;

import java.util.Properties;
import java.util.Random;
import java.util.TreeSet;

public class HyParView extends GenericProtocol implements INodeListener {

    public static final short PROTO_ID = 1;
    public static final String ALG_NAME = "HyParView";
    private static final int LOG_N_PLUS_C = 10;
    private TreeSet<Host> activeView;
    private TreeSet<Host> passiveView;
    public int k, c;

    public HyParView(INetwork net) throws Exception {
        super(ALG_NAME, PROTO_ID, net);
        registerMessageHandler(JoinMessage.MSG_CODE, null, JoinMessage.serializer);
    }

    @Override
    public void init(Properties properties) {
        this.activeView = new TreeSet<>();
    }

    private final ProtocolMessageHandler uponReceiveJoin = new ProtocolMessageHandler() {
        @Override
        public void receive(ProtocolMessage msg) {
            JoinMessage joinMessage = (JoinMessage) msg;
            Host node = joinMessage.getNode();
            addNodeToActiveView(node);
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
                dropRandomNodeFromActiveView();
            }
            passiveView.add(host);
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

}
