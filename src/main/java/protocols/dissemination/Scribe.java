package protocols.dissemination;

import babel.exceptions.DestinationProtocolDoesNotExist;
import babel.handlers.*;
import babel.notification.INotificationConsumer;
import babel.protocol.GenericProtocol;
import babel.protocol.event.ProtocolMessage;
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
import utils.PropertiesUtils;

import java.util.*;

public class Scribe extends GenericProtocol implements INotificationConsumer {
    final static Logger logger = LogManager.getLogger(Scribe.class.getName());

    public static final short PROTOCOL_ID = 14153;
    public static final String PROTOCOL_NAME = "Scribe";

    private static final String SUBSCRIPTION_TTL = "subscriptionTTL";
    private static final String RECYCLE_SUBSCRIPTIONS_INIT = "recycleSubscriptionsInit";
    private static final String RECYCLE_SUBSCRIPTIONS_PERIOD = "recycleSubscriptionsPeriod";

    private Map<String, Set<HostSubscription>> downTopicTree;
    private Map<String, Set<HostSubscription>> upTopicTree;
    private int subscriptionTTL;
    private Set<String> myTopics;

    private final ProtocolTimerHandler uponRecycleSubscribes = (ProtocolTimer) -> {
        for (String topic : downTopicTree.keySet()) {
            Set<HostSubscription> downTopicBranch = downTopicTree.getOrDefault(topic, new HashSet<>());
            for (HostSubscription leaf : downTopicBranch) {
                Host host = leaf.getHost();
                if (leaf.isTimeExpired(subscriptionTTL)) {
                    logger.info(String.format("Subscription of %s to topic %s expired", host, topic));
                    if (!host.equals(myself) && !myTopics.contains(topic)) {
                        removeFromDownTopicTree(topic, host);
                    }
                }
            }

            if (!downTopicBranch.isEmpty()) {
                Set<HostSubscription> upTopicBranch = upTopicTree.get(topic);
                ScribeMessage message = new ScribeMessage(topic, true, myself);
                if (!isBranchEmpty(upTopicBranch)) {
                    sendToBranch(upTopicBranch, message);
                }
            } else {
                Set<HostSubscription> upTopicBranch = upTopicTree.remove(topic);
                ScribeMessage message = new ScribeMessage(topic, false, myself);
                if (!isBranchEmpty(upTopicBranch)) {
                    sendToBranch(upTopicBranch, message);
                } else {
                    requestRoute(message);
                }
            }
        }
    };

    private final ProtocolReplyHandler uponRouteOk = (protocolReply) -> {
        RouteOk routeOk = (RouteOk) protocolReply;
        String topic = routeOk.getTopic();
        Host forwardedTo = routeOk.getForwardedTo();

        logger.info(String.format("[%s] Received route ok to %s", myself, forwardedTo));
        Set<HostSubscription> downTopicBranch = downTopicTree.getOrDefault(topic, new HashSet<>());

        if (!downTopicBranch.contains(createTempSubscription(forwardedTo))) {
            addToUpTopicTree(topic, forwardedTo);
        }
    };
    private final ProtocolMessageHandler uponScribeMessage = (protocolMessage) -> {
        ScribeMessage message = (ScribeMessage) protocolMessage;
        processMessage(message);
    };
    /**
     * Handler dissemination request from the upper level
     */
    private ProtocolRequestHandler uponDisseminateSubRequest = (protocolRequest) -> {
        DisseminateSubRequest request = (DisseminateSubRequest) protocolRequest;
        String topic = request.getTopic();
        boolean subscribe = request.isSubscribe();

        if (subscribe) {
            if (!isTopicInTree(topic)) {
                ScribeMessage message = new ScribeMessage(topic, true, myself);
                requestRoute(message);
            }

            myTopics.add(topic);
            addToDownTopicTree(topic, myself);
        } else {
            myTopics.remove(topic);
            removeFromDownTopicTree(topic, myself);

            Set<HostSubscription> downTopicBranch = downTopicTree.get(topic);
            if (isBranchEmpty(downTopicBranch)) {
                ScribeMessage message = new ScribeMessage(topic, false, myself);
                Set<HostSubscription> upTopicBranch = upTopicTree.get(topic);

                if (isBranchEmpty(upTopicBranch)) {
                    requestRoute(message);
                } else {
                    sendToBranch(upTopicBranch, message);
                }
            }
        }
    };
    private ProtocolRequestHandler uponDisseminatePubRequest = (protocolRequest) -> {
        DisseminatePubRequest request = (DisseminatePubRequest) protocolRequest;
        String topic = request.getTopic();
        String messageText = request.getMessage();

        if (subscribedTo(topic)) {
            triggerNotification(new MessageDeliver(topic, messageText));
        }

        Set<HostSubscription> downTopicBranch = downTopicTree.get(topic);
        Set<HostSubscription> upTopicBranch = upTopicTree.get(topic);

        ScribeMessage message = new ScribeMessage(topic, messageText, myself);
        if (noBranches(downTopicBranch, upTopicBranch)) {
            addToDownTopicTree(topic, myself);
            requestRoute(message);
        } else {
            Set<Host> exclude = new HashSet<>();
            exclude.add(myself);
            if (downTopicBranch != null) {
                sendToBranch(downTopicBranch, message, exclude);
            }
            if (upTopicBranch != null) {
                sendToBranch(upTopicBranch, message, exclude);
            }
        }
    };

    public Scribe(INetwork net) throws Exception {
        super(PROTOCOL_NAME, PROTOCOL_ID, net);

        registerNotification(MessageDeliver.NOTIFICATION_ID, MessageDeliver.NOTIFICATION_NAME);

        registerNotificationHandler(Chord.PROTOCOL_ID, RouteDelivery.NOTIFICATION_ID, uponRouteDelivery);

        registerRequestHandler(DisseminatePubRequest.REQUEST_ID, uponDisseminatePubRequest);
        registerRequestHandler(DisseminateSubRequest.REQUEST_ID, uponDisseminateSubRequest);
        registerReplyHandler(RouteOk.REPLY_ID, uponRouteOk);

        registerMessageHandler(ScribeMessage.MSG_CODE, uponScribeMessage, ScribeMessage.serializer);

        registerTimerHandler(RecycleSubscribesTimer.TimerCode, uponRecycleSubscribes);
    }

    @Override
    public void init(Properties properties) {
        logger.info("Scribe Starting on " + myself.toString());
        subscriptionTTL = PropertiesUtils.getPropertyAsInt(properties, SUBSCRIPTION_TTL);

        this.downTopicTree = new HashMap<>();
        this.upTopicTree = new HashMap<>();
        this.myTopics = new HashSet<>();

        setupPeriodicTimer(new RecycleSubscribesTimer(),
                PropertiesUtils.getPropertyAsInt(properties, RECYCLE_SUBSCRIPTIONS_INIT),
                PropertiesUtils.getPropertyAsInt(properties, RECYCLE_SUBSCRIPTIONS_PERIOD));
    }

    private boolean isTopicInTree(String topic) {
        return upTopicTree.containsKey(topic) || downTopicTree.containsKey(topic);
    }

    private boolean isBranchEmpty(Set<HostSubscription> tree) {
        return tree == null || tree.isEmpty();
    }

    private void sendToBranch(Set<HostSubscription> branch, ProtocolMessage message) {
        for (HostSubscription leaf : branch) {
            sendMessageSideChannel(message, leaf.getHost());
        }
    }

    private void sendToBranch(Set<HostSubscription> branch, ProtocolMessage message, Set<Host> exclude) {
        for (HostSubscription leaf : branch) {
            Host leafHost = leaf.getHost();
            if (!exclude.contains(leafHost)) {
                sendMessageSideChannel(message, leafHost);
            }
        }
    }

    private final ProtocolNotificationHandler uponRouteDelivery = (notification) -> {
        logger.info(String.format("Received %s route request", myself));
        RouteDelivery deliver = (RouteDelivery) notification;
        processMessage((ScribeMessage) deliver.getMessage());
    };

    private boolean noBranches(Set<HostSubscription> downBranch, Set<HostSubscription> upBranch) {
        return downBranch == null && upBranch == null;
    }

    private HostSubscription createTempSubscription(Host host) {
        return new HostSubscription(host, System.currentTimeMillis());
    }

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
        logger.info(String.format("[%s] Processing %s", myself, message));

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
        String messageText = scribeMessage.getMessage();
        Host host = scribeMessage.getHost();

        if (subscribedTo(topic)) {
            triggerNotification(new MessageDeliver(topic, messageText));
        }

        Set<HostSubscription> downTopicBranch = downTopicTree.get(topic);
        Set<HostSubscription> upTopicBranch = upTopicTree.get(topic);
        ScribeMessage message = new ScribeMessage(topic, messageText, myself);

        if (noBranches(downTopicBranch, upTopicBranch)) {
            addToDownTopicTree(topic, myself);
            addToDownTopicTree(topic, host);

            if (!host.equals(myself)) {
                requestRoute(message);
            }

            return;
        }

        Set<Host> exclude = new HashSet<>();
        exclude.add(myself);
        exclude.add(host);

        if (upTopicBranch != null) {
            sendToBranch(upTopicBranch, message, exclude);

            if (!upTopicBranch.contains(createTempSubscription(host))) {
                addToDownTopicTree(topic, host);
            }
        }

        if (downTopicBranch != null) {
            sendToBranch(downTopicBranch, message, exclude);
        }
    }

    private void processUnsubscribe(ScribeMessage message) {

        String topic = message.getTopic();
        Host host = message.getHost();

        removeFromDownTopicTree(topic, host);
        removeFromUpTopicTree(topic, host);

        Set<HostSubscription> downTopicBranch = downTopicTree.get(topic);
        if (isBranchEmpty(downTopicBranch) && !host.equals(myself)) {
            Set<HostSubscription> upTopicBranch = upTopicTree.get(topic);
            message = new ScribeMessage(topic, false, myself);
            if (upTopicBranch != null) {
                sendToBranch(upTopicBranch, message);
            } else {
                requestRoute(message);
            }
        }
    }

    private void processSubscribe(ScribeMessage message) {
        String topic = message.getTopic();
        Host host = message.getHost();

        if (!isTopicInTree(topic) && !host.equals(myself)) {
            requestRoute(new ScribeMessage(topic, true, myself));
        }

        Set<HostSubscription> upTopicPeers = upTopicTree.getOrDefault(topic, new HashSet<>());
        if (!upTopicPeers.contains(createTempSubscription(host)) && !host.equals(myself)) {
            addToDownTopicTree(topic, host);
        }
    }

    /**
     * If this process is subscribe to that topic
     * returns <code>true</code> otherwise returns <code>false</code>
     *
     * @return <code>true</code> if subscribed
     */
    private boolean subscribedTo(String topic) {
        return this.myTopics.contains(topic);
    }

    /**
     * Adds host to topic tree if it already exists
     * updates the time of his subscription
     *
     * @param topic
     * @param host
     */
    private void addToDownTopicTree(String topic, Host host) {
        addToTopicTree(topic, host, downTopicTree);
    }

    private void addToUpTopicTree(String topic, Host host) {
        addToTopicTree(topic, host, this.upTopicTree);
    }

    private void addToTopicTree(String topic, Host host, Map<String, Set<HostSubscription>> tree) {
        Set<HostSubscription> hostSet;

        if (!tree.containsKey(topic)) {
            tree.put(topic, new HashSet<>());
        }

        hostSet = tree.get(topic);
        HostSubscription subscription = createTempSubscription(host);
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
    private void removeFromDownTopicTree(String topic, Host host) {
        removeFromTopicTree(topic, host, this.downTopicTree);
    }

    private void removeFromTopicTree(String topic, Host host, Map<String, Set<HostSubscription>> tree) {
        HostSubscription subscription = createTempSubscription(host);
        Set<HostSubscription> hostSet = tree.get(topic);
        if (hostSet != null) {
            hostSet.remove(subscription);

            if (hostSet.isEmpty()) {
                tree.remove(topic);
            }
        }
    }

    private void removeFromUpTopicTree(String topic, Host host) {
        removeFromTopicTree(topic, host, this.upTopicTree);
    }

}
