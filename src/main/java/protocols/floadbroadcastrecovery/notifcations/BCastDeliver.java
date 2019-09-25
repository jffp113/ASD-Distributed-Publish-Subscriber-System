package protocols.floadbroadcastrecovery.notifcations;

import babel.notification.ProtocolNotification;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BCastDeliver extends ProtocolNotification {

    public static final short NOTIFICATION_ID = 201;
    public static final String NOTIFICATION_NAME = "BcastDeliver";
    public static final String REGEX = "^(\\d+)\\|(\\d+)\\|(.*)";
    private Pattern regex = Pattern.compile(REGEX);

    private String message;
    private String topic;

    public BCastDeliver(String message, String topic) {
        super(BCastDeliver.NOTIFICATION_ID, NOTIFICATION_NAME);
        this.message = message;
        this.topic = topic;

    }

    public BCastDeliver(byte[] payload) {
        super(BCastDeliver.NOTIFICATION_ID, NOTIFICATION_NAME);
        String payloadAsAString = new String(payload);
        Matcher m = regex.matcher(payloadAsAString);
        int topicLength = -1;
        int messageLength = -1;
        String all = "";

        if (m.find()) {
            topicLength = Integer.parseInt(m.group(1));
            messageLength = Integer.parseInt(m.group(2));
            all = m.group(3);
        }

        this.topic = all.substring(0, topicLength);
        this.message = all.substring(topicLength);

    }

    public String getMessage() {
        return message;
    }

    public String getTopic() {
        return topic;
    }


}
