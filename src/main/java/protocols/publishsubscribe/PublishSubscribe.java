package protocols.publishsubscribe;

import babel.handlers.ProtocolRequestHandler;
import babel.notification.INotificationConsumer;
import babel.notification.ProtocolNotification;
import babel.protocol.GenericProtocol;
import network.INetwork;
import protocols.floadbroadcastrecovery.GossipBCast;
import protocols.floadbroadcastrecovery.notifcations.BCastDeliver;
import protocols.floadbroadcastrecovery.requests.BCastRequest;
import protocols.publishsubscribe.notifications.PBDeliver;
import protocols.publishsubscribe.requests.PublishRequest;
import protocols.publishsubscribe.requests.SubscribeRequest;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

//TODO implement this protocol with the correspondent methods
public class PublishSubscribe extends GenericProtocol implements INotificationConsumer {

    public final static short PROTOCOL_ID = 1000;
    public final static String PROTOCOL_NAME = "Publish/Subscriber";
    public static final int INITIAL_CAPACITY = 100;
    private Map<String, Boolean> topics;

    public PublishSubscribe(INetwork net) throws Exception {
        super(PROTOCOL_NAME, PROTOCOL_ID, net);

        // Notifications produced
        registerNotification(PBDeliver.NOTIFICATION_ID, PBDeliver.NOTIFICATION_NAME);

        // Requests
        registerRequestHandler(PublishRequest.REQUEST_ID, uponPublishRequest);
        registerRequestHandler(SubscribeRequest.REQUEST_ID,uponSubscribeRequest);

        //registerNotification(BCastDeliver.NOTIFICATION_ID,BCastDeliver.NOTIFICATION_NAME);
        subscribeNotification(BCastDeliver.NOTIFICATION_ID,this);
    }

    @Override
    public void init(Properties properties) {
        this.topics = new HashMap<>(INITIAL_CAPACITY);
    }

    private ProtocolRequestHandler uponSubscribeRequest = (protocolRequest) -> {
        SubscribeRequest subscribeRequest = (SubscribeRequest) protocolRequest;
        String topic = subscribeRequest.getTopic();
        boolean isSubscribe = subscribeRequest.isSubscribe();
        if(this.topics.get(topic)==null){
            if(isSubscribe){
                this.topics.put(topic,true);
            }
        }else{
            if(!isSubscribe){
                this.topics.remove(topic);
            }
        }

    };

    private ProtocolRequestHandler uponPublishRequest = (publishRequest) -> {
        PublishRequest pbReq = (PublishRequest) publishRequest;
        BCastRequest bCastRequest = new BCastRequest(pbReq.getMessage(),pbReq.getTopic());
        bCastRequest.setDestination(GossipBCast.PROTOCOL_ID);
        try{
            this.sendRequest(bCastRequest);
        }catch(Exception e){
            e.printStackTrace();
        }

    };

    public void deliverNotification (ProtocolNotification bCastDeliver){
        BCastDeliver bCastDel = (BCastDeliver) bCastDeliver;
        String topic = bCastDel.getTopic();

        if (this.topics.get(topic)!=null){
            triggerNotification(new PBDeliver(bCastDel.getMessage(), topic));
        }

    }
}
