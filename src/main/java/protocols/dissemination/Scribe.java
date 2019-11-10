package protocols.dissemination;

import babel.exceptions.DestinationProtocolDoesNotExist;
import babel.handlers.ProtocolReplyHandler;
import babel.handlers.ProtocolRequestHandler;
import babel.handlers.ProtocolTimerHandler;
import babel.protocol.GenericProtocol;
import network.Host;
import network.INetwork;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.dht.Chord;
import protocols.dht.messagesTopics.RefreshTopicsMessage;
import protocols.dht.notifications.MessageDeliver;
import protocols.dht.requests.RouteRequest;
import protocols.dissemination.message.DeliverMessage;
import protocols.dissemination.requests.DisseminateRequest;
import protocols.dissemination.requests.RouteDeliver;
import protocols.dissemination.timers.RecycleSubscribesTimer;
import protocols.dissemination.timers.RefreshSubscribesTimer;
import utils.PropertiesUtils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Scribe extends GenericProtocol {
    final static Logger logger = LogManager.getLogger(Scribe.class.getName());

    public static final short PROTOCOL_ID = 14153;
    public static final String PROTOCOL_NAME = "Scribe";

    private static final String SUBSCRIPTION_TTL = "subscriptionTTL";
    private static final String REFRESH_SUBSCRIBES_INIT = "refreshSubscribesInit";
    private static final String REFRESH_SUBSCRIBES_PERIOD = "refreshSubscribesPeriod";
    private static final String RECYCLE_SUBSCRIPTIONS_INIT = "recycleSubscriptionsInit";
    private static final String RECYCLE_SUBSCRIPTIONS_PERIOD = "recycleSubscriptionsPeriod";

    private Map<String, Set<HostSubscription>> topicTree;
    private Set<String> topicSubs;
    private int subscriptionTTL;

    public Scribe(INetwork net) throws Exception {
        super(PROTOCOL_NAME, PROTOCOL_ID, net);

        registerReplyHandler(RouteDeliver.REQUEST_ID, uponRouteDeliver);
        registerRequestHandler(DisseminateRequest.REQUEST_ID, uponDisseminateRequest);

        registerTimerHandler(RecycleSubscribesTimer.TimerCode, uponRecycleSubscribes);
        registerTimerHandler(RefreshSubscribesTimer.TimerCode, uponRefreshSubscribes);
    }

    @Override
    public void init(Properties properties) {
        logger.info("Scribe Starting on " + myself.toString());
        registerNotification(MessageDeliver.NOTIFICATION_ID, MessageDeliver.NOTIFICATION_NAME);
        subscriptionTTL = PropertiesUtils.getPropertyAsInt(properties, SUBSCRIPTION_TTL);

      /*  setupPeriodicTimer(new RecycleSubscribesTimer(),
                PropertiesUtils.getPropertyAsInt(properties, RECYCLE_SUBSCRIPTIONS_INIT),
                PropertiesUtils.getPropertyAsInt(properties, RECYCLE_SUBSCRIPTIONS_PERIOD));
        setupPeriodicTimer(new RefreshSubscribesTimer(),
                PropertiesUtils.getPropertyAsInt(properties, REFRESH_SUBSCRIBES_INIT),
                PropertiesUtils.getPropertyAsInt(properties, REFRESH_SUBSCRIBES_PERIOD));
*/
        this.topicTree = new ConcurrentHashMap<>();
        this.topicSubs = Collections.synchronizedSet(new HashSet<>());
    }

    private final ProtocolTimerHandler uponRecycleSubscribes = (ProtocolTimer) -> {
        for (String topic : topicTree.keySet()) {
            Set<HostSubscription> hostSubscriptions = new HashSet<>(topicTree.get(topic));
            for (HostSubscription hostSubscription : hostSubscriptions) {
                if (hostSubscription.isTimeExpired(subscriptionTTL)) {
                    logger.info(String.format("Subscription of %s to topic %s expired", hostSubscription.getHost(), topic));
                    removeFromTopics(topic, hostSubscription.getHost());
                }
            }
        }
    };

    private final ProtocolTimerHandler uponRefreshSubscribes = (ProtocolTimer) -> {
        logger.info(String.format("Refreshing subscribes of %s, topics: %s", myself, topicSubs));
        for (String topic : topicSubs) {
            requestRoute(new DeliverMessage(topic, true, myself));
        }
    };

    private final ProtocolReplyHandler uponRouteDeliver = (request) -> {
        logger.info(String.format("Received %s route request", myself));
        RouteDeliver deliver = (RouteDeliver) request;
        processMessage(deliver);
    };

    private void requestRoute(DeliverMessage message) {
        RouteRequest routeRequest = new RouteRequest(message, message.getTopic());
        routeRequest.setDestination(Chord.PROTOCOL_ID);
        try {
            sendRequest(routeRequest);
        } catch (DestinationProtocolDoesNotExist destinationProtocolDoesNotExist) {
            destinationProtocolDoesNotExist.printStackTrace();
        }
    }

    private void processMessage(RouteDeliver deliver) {
        DeliverMessage message = (DeliverMessage) deliver.getMessageDeliver();

        switch (message.getMessageType()) {
            case SUBSCRIBE:
                processSubscribe(message);
                break;
            case UNSUBSCRIBE:
                processUnsubscribe(message);
                break;
            case PUBLICATION:
                processPublication(message);
                break;
            default: //Drop Message process
                logger.warn(String.format("Dropped message with messageType %s", message.getMessageType()));
        }
    }

    private void processPublication(DeliverMessage message) {
        String topic = message.getTopic();
        Set<HostSubscription> hostSubscriptionSet = topicTree.get(topic);
        if(hostSubscriptionSet != null){
            Set<HostSubscription> hostSubscriptions = new HashSet<>(hostSubscriptionSet);
            for (HostSubscription host : hostSubscriptions) {
                if (!host.getHost().equals(myself))
                    sendMessageSideChannel(message, host.getHost());
            }
        }

        if (subscribedTo(topic)) {
            triggerNotification(new MessageDeliver(topic, message.getMessage()));
        }

    }

    private void processUnsubscribe(DeliverMessage message) {
        removeFromTopics(message.getTopic(), message.getHost());
        requestRoute(message);
    }

    private void processSubscribe(DeliverMessage message) {
        if (!topicTree.containsKey(message.getTopic())) {
            requestRoute(message);
        }
        addToTopics(message.getTopic(), message.getFrom());
    }

    /**
     * If this process is subscribe to that topic
     * returns <code>true</code> otherwise returns <code>false</code>
     *
     * @return <code>true</code> if subscribed
     */
    private boolean subscribedTo(String topic) {
        return this.topicSubs.contains(topic);
    }

    /**
     * Adds host to topic tree if it already exists
     * updates the time of his subscription
     *
     * @param topic
     * @param host
     */
    private void addToTopics(String topic, Host host) {
        Set<HostSubscription> hostSet;
        if (!topicTree.containsKey(topic)) {
            topicTree.put(topic, new HashSet<>());
        }
        hostSet = topicTree.get(topic);
        HostSubscription subscription = new HostSubscription(host, System.currentTimeMillis());
        hostSet.remove(subscription);
        hostSet.add(subscription);
    }

    /**
     * Remove Host from topic Tree
     * if no hosts are in that topic tree
     * remove tree
     *
     * @param topic
     * @param host
     */
    private void removeFromTopics(String topic, Host host) {
        HostSubscription subscription = new HostSubscription(host, System.currentTimeMillis());
        Set<HostSubscription> hostSet = topicTree.get(topic);
        hostSet.remove(subscription);

        if (hostSet.isEmpty()) {
            topicTree.remove(topic);
        }
    }

    /**
     * Handler dissemination request from the upper level
     */
    private ProtocolRequestHandler uponDisseminateRequest = (protocolRequest) -> {
        DisseminateRequest request = (DisseminateRequest) protocolRequest;
        DeliverMessage message = (DeliverMessage) request.getMessage();

        if (message.getMessageType().equals(MessageType.SUBSCRIBE)) {
            addToTopics(message.getTopic(), myself);
            topicSubs.add(message.getTopic());
        }
        requestRoute(message);
    };

}
