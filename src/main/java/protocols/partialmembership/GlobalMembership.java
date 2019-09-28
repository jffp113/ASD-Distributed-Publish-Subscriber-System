package protocols.partialmembership;


import babel.exceptions.DestinationProtocolDoesNotExist;
import babel.exceptions.HandlerRegistrationException;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.partialmembership.messages.GossipProtocolMessage;
import protocols.partialmembership.notifications.GlobalMembershipNotification;
import protocols.partialmembership.requests.GetSampleReply;
import protocols.partialmembership.requests.GetSampleRequest;
import protocols.partialmembership.timers.EchoProtocolTimer;
import protocols.partialmembership.timers.GossipProtocolTimer;

import java.net.InetAddress;
import java.util.*;

public class GlobalMembership extends GenericProtocol implements INodeListener {

    //Numeric identifier of the protocol
    public final static short PROTOCOL_ID = 100;
    public final static String PROTOCOL_NAME = "Global Membership";
    private Set<Host> peers;
    private short sampleSize;

    private static final Logger logger = LogManager.getLogger(GlobalMembership.class);

    public GlobalMembership(INetwork net) throws HandlerRegistrationException {
        super(PROTOCOL_NAME, GlobalMembership.PROTOCOL_ID, net);

        //Declare Messages sent/received of the protocol
        registerMessageHandler(GossipProtocolMessage.MSG_CODE, uponGossipMessage, GossipProtocolMessage.serializer);

        //Declare Timers of the Protocol
        registerTimerHandler(GossipProtocolTimer.TimerCode, uponGossipProtocolTimer);
        registerTimerHandler(EchoProtocolTimer.TimerCode, uponEchoProtocolTimer);

        //Declare Notifications issued by the protocol
        this.registerNotification(GlobalMembershipNotification.NOTIFICATION_ID, GlobalMembershipNotification.NOTIFICATION_NAME);

        //declare Notifications consumed by the protocol

        //declare requests exposed by the protocol
        registerRequestHandler(GetSampleRequest.REQUEST_ID, uponGetSampleRequest);

        //declare replies consumed by the protocol
    }

    @Override
    public void init(Properties props) {

        //Setup configuration of the protocol
        registerNodeListener(this);
        this.sampleSize = Short.parseShort(props.getProperty("SampleSize", "5"));
        this.peers = new HashSet<>();
        if (props.containsKey("Contact")) {
            try {
                String[] hostElems = props.getProperty("Contact").split(":");
                Host contact = new Host(InetAddress.getByName(hostElems[0]), Short.parseShort(hostElems[1]));
                addNetworkPeer(contact);
            } catch (Exception e) {
                System.err.println("Invalid contact on configuration: '" + props.getProperty("Contact"));
            }
        }

        //Setup timers
        UUID gossipTimerUUID = setupPeriodicTimer(new GossipProtocolTimer(), 1000, 1000);
        UUID echoTimerUUID = setupPeriodicTimer(new EchoProtocolTimer(), 1000, 5000);
    }


    private final ProtocolMessageHandler uponGossipMessage = new ProtocolMessageHandler() {
        @Override
        public void receive(ProtocolMessage msg) {
            Set<Host> sample = ((GossipProtocolMessage) msg).getSample();
            sample.forEach(s -> addNetworkPeer(s));
        }
    };

    private ProtocolTimerHandler uponGossipProtocolTimer = new ProtocolTimerHandler() {
        @Override
        public void uponTimer(ProtocolTimer timer) {
            LinkedList<Host> m = new LinkedList<>(peers);
            if (m.size() >= 1) {
                Random r = new Random();

                while (m.size() > sampleSize) {
                    m.remove(r.nextInt(m.size()));
                }
                Host destination = m.remove(r.nextInt(m.size()));
                m.add(myself);
                GossipProtocolMessage gm = new GossipProtocolMessage(m);
                //network.sendMessage(gm.getId(), gm, destination);
                sendMessage(gm, destination);
            }
        }
    };

    private ProtocolTimerHandler uponEchoProtocolTimer = new ProtocolTimerHandler() {
        @Override
        public void uponTimer(ProtocolTimer timer) {
            GlobalMembershipNotification n = new GlobalMembershipNotification(peers);
            triggerNotification(n);
        }
    };

    private ProtocolRequestHandler uponGetSampleRequest = new ProtocolRequestHandler() {
        @Override
        public void uponRequest(ProtocolRequest request) {
            GetSampleRequest req = (GetSampleRequest) request;

            //Compute answer
            Random rand = new Random();
            List<Host> toSend = new ArrayList<>(peers);
            while (toSend.size() > req.getFanout()) {
                toSend.remove(rand.nextInt(toSend.size()));
            }

            //Send reply
            GetSampleReply reply = new GetSampleReply(req.getIdentifier(), new HashSet<Host>(toSend));
            reply.invertDestination(req);
            try {
                sendReply(reply);
            } catch (DestinationProtocolDoesNotExist destinationProtocolDoesNotExist) {
                destinationProtocolDoesNotExist.printStackTrace();
                System.exit(1);
            }
        }
    };

    //Implementation of the INodeListener interface

    @Override
    public void nodeDown(Host peer) {
        System.out.println("node down: " + peer);
        peers.remove(peer);
    }

    @Override
    public void nodeUp(Host peer) {
        System.out.println("node up: " + peer);
        peers.add(peer);
    }

    @Override
    public void nodeConnectionReestablished(Host peer) {
        System.out.println("connection reestablished: " + peer);
    }


}
