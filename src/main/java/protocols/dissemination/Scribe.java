package protocols.dissemination;

import babel.exceptions.DestinationProtocolDoesNotExist;
import babel.handlers.ProtocolReplyHandler;
import babel.handlers.ProtocolRequestHandler;
import babel.protocol.GenericProtocol;
import babel.protocol.event.ProtocolMessage;
import network.Host;
import network.INetwork;
import protocols.dht.ChordWithSalt;
import protocols.dissemination.message.DeliverMessage;
import protocols.dht.notifications.MessageDeliver;
import protocols.dissemination.requests.DisseminateRequest;
import protocols.dissemination.requests.RouteDeliver;
import protocols.dht.requests.RouteRequest;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class Scribe extends GenericProtocol {

    public static final short PROTOCOL_ID = 14153;
    public static final String PROTOCOL_NAME = "Scribe";

    private HashMap<String, Set<HostSubscription>> topicTree;
    private Set<String> topicSubs;

    public Scribe(INetwork net) throws Exception {
        super(PROTOCOL_NAME,PROTOCOL_ID, net);
        registerReplyHandler(RouteRequest.REQUEST_ID, uponRouteDeliver);
        registerRequestHandler(DisseminateRequest.REQUEST_ID,uponDisseminateRequest);
    }

    @Override
    public void init(Properties properties) {
        registerNotification(MessageDeliver.NOTIFICATION_ID,MessageDeliver.NOTIFICATION_NAME);
        this.topicTree = new HashMap<>();
        this.topicSubs = new HashSet<>();
    }

    private final ProtocolReplyHandler uponRouteDeliver = (request) -> {
        RouteDeliver deliver = (RouteDeliver)request;
        processMessage(deliver);
        requestRoute(deliver.getMessageDeliver());
    };

    private void requestRoute(ProtocolMessage message){
        RouteRequest routeRequest = new RouteRequest(message);
        routeRequest.setDestination(ChordWithSalt.PROTOCOL_ID);
        try {
            sendRequest(routeRequest);
        } catch (DestinationProtocolDoesNotExist destinationProtocolDoesNotExist) {
            destinationProtocolDoesNotExist.printStackTrace();
        }
    }

    private void processMessage(RouteDeliver deliver) {
        DeliverMessage message = (DeliverMessage)deliver.getMessageDeliver();

        switch (message.getMessageType()){
            case SUBSCRIBE: processSubscribe(message); break;
            case UNSUBSCRIBE: processUnsubscribe(message); break;
            case PUBLICATION: processPublication(message); break;
            default: //Drop Message process
        }
    }

    private void processPublication(DeliverMessage message) {
        for(HostSubscription host : topicTree.get(message.getTopic())){
            sendMessageSideChannel(message,host.getHost());
        }

        if(this.topicSubs.contains(message.getTopic())){
            triggerNotification(new MessageDeliver(message.getTopic(), message.getMessage()));
        }
    }

    private void processUnsubscribe(DeliverMessage message) {
        removeFromTopics(message.getTopic(),message.getHost());
    }

    private void processSubscribe(DeliverMessage message) {
        addToTopics(message.getTopic(),message.getHost());
    }

    /**
     * If this process is subscribe to that topic
     * returns <code>true</code> otherwise returns <code>false</code>
     * @return <code>true</code> if subscribed
     */
    private boolean subscribedTo(String topic) {
        return this.topicSubs.contains(topic);
    }

    /**
     * Adds host to topic tree if it already exists
     * updates the time of his subscription
     * @param topic
     * @param host
     */
    private void addToTopics(String topic,Host host){
        Set<HostSubscription> hostSet;
        if(!topicTree.containsKey(topic)){
            topicTree.put(topic, new HashSet<>());
        }
        hostSet = topicTree.get(topic);
        HostSubscription subscription = new HostSubscription(host,System.currentTimeMillis());
        hostSet.remove(subscription);
        hostSet.add(subscription);
    }

    /**
     * Remove Host from topic Tree
     * if no hosts are in that topic tree
     * remove tree
     * @param topic
     * @param host
     */
    private void removeFromTopics(String topic,Host host){
        HostSubscription subscription = new HostSubscription(host,System.currentTimeMillis());
        Set<HostSubscription> hostSet = topicTree.get(topic);
        hostSet.remove(subscription);

        if(hostSet.isEmpty()){
            topicTree.remove(topic);
        }
    }

    /**
     * Handler dissemination request from the upper level
     */
    private ProtocolRequestHandler uponDisseminateRequest = (protocolRequest) -> {
        DisseminateRequest request = (DisseminateRequest)protocolRequest;
        DeliverMessage message = (DeliverMessage)request.getMessage();

        if(message.getMessageType().equals(MessageType.SUBSCRIBE)){
            addToTopics(message.getTopic(),myself);
            topicSubs.add(message.getTopic());
        }
        requestRoute(message);
    };

}
