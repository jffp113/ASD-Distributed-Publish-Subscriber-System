package protocols.publicsubscriber.messages.notifications;

import babel.notification.ProtocolNotification;

public class PBDeliver extends ProtocolNotification {
    public static short NOTIFICATION_ID = 3;
    public static String NOTIFICATION_NAME = "PBDeliver";
    private String message;

    public PBDeliver(String message) {
        super(NOTIFICATION_ID, NOTIFICATION_NAME);
        this.message=message;
    }

    public String getMessage() {
        return message;
    }

}
