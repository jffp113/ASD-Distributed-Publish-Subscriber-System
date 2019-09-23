package protocols.floadbroadcastrecovery.notifcations;

import babel.notification.ProtocolNotification;

public class BCastDeliver extends ProtocolNotification {

    public static final short NOTIFICATION_ID = 201;
    public static final String NOTIFICATION_NAME = "BcastDeliver";

    private String message;
    private String topic;

    public BCastDeliver(String message, String topic) {
        super(BCastDeliver.NOTIFICATION_ID, NOTIFICATION_NAME);
        this.message=message;
        this.topic=topic;

    }

    public String getMessage() {
        return message;
    }

    public String getTopic() {
        return topic;
    }
}
