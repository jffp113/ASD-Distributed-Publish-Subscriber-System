package protocols.dissemination;

import babel.exceptions.DestinationProtocolDoesNotExist;
import babel.handlers.*;
import babel.notification.INotificationConsumer;
import babel.protocol.GenericProtocol;
import babel.protocol.INotificationProducer;
import network.Host;
import network.INetwork;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.dht.Chord;
import protocols.dht.notifications.MessageDeliver;
import protocols.dht.requests.RouteRequest;
import protocols.dissemination.message.ScribeMessage;
import protocols.dissemination.notifications.RouteDelivery;
import protocols.dissemination.requests.DisseminatePubRequest;
import protocols.dissemination.requests.DisseminateSubRequest;
import protocols.dissemination.requests.RouteOk;
import protocols.dissemination.timers.RecycleSubscribesTimer;
import protocols.dissemination.timers.RefreshSubscribesTimer;
import utils.PropertiesUtils;

import java.util.*;

public class Scribe extends GenericProtocol implements INotificationConsumer {
    final static Logger logger = LogManager.getLogger(Scribe.class.getName());

    public static final short PROTOCOL_ID = 14153;
    public static final String PROTOCOL_NAME = "Scribe";

    private static final String SUBSCRIPTION_TTL = "subscriptionTTL";
    private static final String REFRESH_SUBSCRIBES_INIT = "refreshSubscribesInit";
    private static final String REFRESH_SUBSCRIBES_PERIOD = "refreshSubscribesPeriod";
    private static final String RECYCLE_SUBSCRIPTIONS_INIT = "recycleSubscriptionsInit";
    private static final String RECYCLE_SUBSCRIPTIONS_PERIOD = "recycleSubscriptionsPeriod";

    private Map<String, Set<HostSubscription>> topicTree;
    private Set<String> topics;
    private int subscriptionTTL;

    public Scribe(INetwork net) throws Exception {
        super(PROTOCOL_NAME, PROTOCOL_ID, net);

        registerRequestHandler(DisseminatePubRequest.REQUEST_ID, uponDisseminatePubRequest);
        registerRequestHandler(DisseminateSubRequest.REQUEST_ID, uponDisseminateSubRequest);
        registerNotificationHandler(Chord.PROTOCOL_ID,RouteDelivery.NOTIFICATION_ID, uponRouteDelivery);
        registerReplyHandler(RouteOk.REPLY_ID, uponRouteOk);

        registerMessageHandler(ScribeMessage.MSG_CODE, uponScribeMessage, ScribeMessage.serializer);

        registerNotification(MessageDeliver.NOTIFICATION_ID, MessageDeliver.NOTIFICATION_NAME);

        registerTimerHandler(RecycleSubscribesTimer.TimerCode, uponRecycleSubscribes);
        registerTimerHandler(RefreshSubscribesTimer.TimerCode, uponRefreshSubscribes);

    }

    @Override
    public void init(Properties properties) {
        logger.info("Scribe Starting on " + myself.toString());
        subscriptionTTL = PropertiesUtils.getPropertyAsInt(properties, SUBSCRIPTION_TTL);

        this.topicTree = new HashMap<>();
        this.topics = new HashSet<>();

//        setupPeriodicTimer(new RecycleSubscribesTimer(),
//                PropertiesUtils.getPropertyAsInt(properties, RECYCLE_SUBSCRIPTIONS_INIT),
//                PropertiesUtils.getPropertyAsInt(properties, RECYCLE_SUBSCRIPTIONS_PERIOD));
//        setupPeriodicTimer(new RefreshSubscribesTimer(),
//                PropertiesUtils.getPropertyAsInt(properties, REFRESH_SUBSCRIBES_INIT),
//                PropertiesUtils.getPropertyAsInt(properties, REFRESH_SUBSCRIBES_PERIOD));

    }

    /**
     * Handler dissemination request from the upper level
     */
    private ProtocolRequestHandler uponDisseminateSubRequest = (protocolRequest) -> {
        DisseminateSubRequest request = (DisseminateSubRequest) protocolRequest;
        String topic = request.getTopic();
        boolean subscribe = request.isSubscribe();

        if (subscribe) {

            topics.add(topic);

            if (!topicTree.containsKey(topic)) {
                ScribeMessage message = new ScribeMessage(topic, true, myself);

                requestRoute(message);
            }

            addToTopicTree(topic, myself);

        } else {
            topics.remove(topic);
            removeFromTopicTree(topic, myself);

            Set<HostSubscription> peers = topicTree.get(topic);

            ScribeMessage message = new ScribeMessage(topic, false, myself);
            if (peers == null || peers.isEmpty()) {
                requestRoute(message);
            } else {
                for (HostSubscription h : peers) {
                    sendMessageSideChannel(message, h.getHost());
                }
            }
        }
    };

    private ProtocolRequestHandler uponDisseminatePubRequest = (protocolRequest) -> {
        DisseminatePubRequest request = (DisseminatePubRequest) protocolRequest;
        String topic = request.getTopic();
        String message = request.getMessage();

        if (subscribedTo(topic)) {
            triggerNotification(new MessageDeliver(topic, message));
        }

        Set<HostSubscription> peers = topicTree.get(topic);

        if (peers != null) {
            logger.info("Sending message to: " + peers);
            for (HostSubscription hSub : peers) {
                Host h = hSub.getHost();
                if (!h.equals(myself)) {
                    ScribeMessage sMessage = new ScribeMessage(topic, message);
                    sendMessageSideChannel(sMessage, h);
                }
            }

        } else {
            logger.info("Does't know topic");
            addToTopicTree(topic, myself);

            ScribeMessage sMessage = new ScribeMessage(topic, message);
            requestRoute(sMessage);
        }

    };

    private final ProtocolReplyHandler uponRouteOk = (protocolReply) -> {
        RouteOk routeOk = (RouteOk) protocolReply;

        logger.info(String.format("[%s] Received route ok to %s", myself, routeOk.getForwardedTo()));
        addToTopicTree(routeOk.getTopic(), routeOk.getForwardedTo());
    };

    private final ProtocolNotificationHandler uponRouteDelivery = (notification) -> {
        logger.info(String.format("Received %s route request", myself));
        RouteDelivery deliver = (RouteDelivery) notification;
        processMessage((ScribeMessage) deliver.getMessage());
    };

    private final ProtocolTimerHandler uponRecycleSubscribes = (ProtocolTimer) -> {
        for (String topic : topicTree.keySet()) {
            Set<HostSubscription> hostSubscriptions = new HashSet<>(topicTree.get(topic));
            for (HostSubscription hostSubscription : hostSubscriptions) {
                if (hostSubscription.isTimeExpired(subscriptionTTL)) {
                    logger.info(String.format("Subscription of %s to topic %s expired", hostSubscription.getHost(), topic));
                    removeFromTopicTree(topic, hostSubscription.getHost());
                }
            }
        }
    };

    private final ProtocolMessageHandler uponScribeMessage = (protocolMessage) -> {
        ScribeMessage scribeMessage = (ScribeMessage) protocolMessage;

        processMessage(scribeMessage);
    };

    private final ProtocolTimerHandler uponRefreshSubscribes = (ProtocolTimer) -> {
        logger.info(String.format("Refreshing subscribes of %s, topics: %s", myself, topics));
        for (String topic : topics) {
            requestRoute(new ScribeMessage(topic, true, myself));
        }
    };

    private void requestRoute(ScribeMessage message) {
        RouteRequest routeRequest = new RouteRequest(message, message.getTopic());
        routeRequest.setDestination(Chord.PROTOCOL_ID);
        try {
            sendRequest(routeRequest);
        } catch (DestinationProtocolDoesNotExist destinationProtocolDoesNotExist) {
            destinationProtocolDoesNotExist.printStackTrace();
        }
    }

    private void processMessage(ScribeMessage message) {
        logger.info(String.format("[%s] Processing %s" ,myself,message));

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

    private void processPublication(ScribeMessage scribeMessage) {
        String topic = scribeMessage.getTopic();
        String message = scribeMessage.getMessage();

        if (subscribedTo(topic)) {
            triggerNotification(new MessageDeliver(topic, message));
        }

        Set<HostSubscription> peers = topicTree.get(topic);

        if (peers != null) {
            for (HostSubscription hSub : peers) {
                Host h = hSub.getHost();
                if (!h.equals(myself) && !h.equals(scribeMessage.getFrom())) {
                    ScribeMessage sMessage = new ScribeMessage(topic, message);
                    sendMessageSideChannel(sMessage, h);
                }
            }

        } else {
            addToTopicTree(topic, myself);

            if (!scribeMessage.getHost().equals(myself)) {
                ScribeMessage sMessage = new ScribeMessage(topic, message);
                requestRoute(sMessage);
            }
        }

    }

    private void processUnsubscribe(ScribeMessage message) {
        String topic = message.getTopic();

        removeFromTopicTree(topic, message.getHost());

        Set<HostSubscription> peers = topicTree.get(topic);

        if ((peers == null || peers.isEmpty()) && !message.getHost().equals(myself)) {
            requestRoute(new ScribeMessage(topic, false, myself));
        }
    }

    private void processSubscribe(ScribeMessage message) {
        if (!topicTree.containsKey(message.getTopic()) && !message.getHost().equals(myself)) {
            requestRoute(new ScribeMessage(message.getTopic(), true, myself));
        }

        addToTopicTree(message.getTopic(), message.getHost());
    }

    /**
     * If this process is subscribe to that topic
     * returns <code>true</code> otherwise returns <code>false</code>
     *
     * @return <code>true</code> if subscribed
     */
    private boolean subscribedTo(String topic) {
        return this.topics.contains(topic);
    }

    /**
     * Adds host to topic tree if it already exists
     * updates the time of his subscription
     *
     * @param topic
     * @param host
     */
    private void addToTopicTree(String topic, Host host) {
        Set<HostSubscription> hostSet;

        if (!topicTree.containsKey(topic)) {
            topicTree.put(topic, new HashSet<>());
        }


        hostSet = topicTree.get(topic);
        logger.info("Tree of :" + topic + hostSet.toString());
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
    private void removeFromTopicTree(String topic, Host host) {
        HostSubscription subscription = new HostSubscription(host, System.currentTimeMillis());
        Set<HostSubscription> hostSet = topicTree.get(topic);
        hostSet.remove(subscription);

        if (hostSet.isEmpty()) {
            topicTree.remove(topic);
        }
    }

}
