package protocols.publishsubscribe.notifications;

import babel.notification.ProtocolNotification;

public class PSDeliver extends ProtocolNotification {
    public static short NOTIFICATION_ID = 3;
    public static String NOTIFICATION_NAME = "PSDeliver";
    private String message;
    private String topic;

    public PSDeliver(String message, String topic) {
        super(NOTIFICATION_ID, NOTIFICATION_NAME);
        this.message = message;
        this.topic = topic;
    }

    public String getMessage() {
        return message;
    }

    public String getTopic() {
        return topic;
    }
}
