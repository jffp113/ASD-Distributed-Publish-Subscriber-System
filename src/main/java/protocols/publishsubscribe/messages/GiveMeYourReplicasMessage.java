package protocols.publishsubscribe.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;

import java.net.UnknownHostException;
import java.util.UUID;

public class GiveMeYourReplicasMessage extends ProtocolMessage {

    public final static short NOTIFICATION_ID = 102;
    private String topic;

    public static final ISerializer<GiveMeYourReplicasMessage> serializer = new ISerializer<GiveMeYourReplicasMessage>() {
        @Override
        public void serialize(GiveMeYourReplicasMessage PSProtocolMessage, ByteBuf out) {
            out.writeShort(PSProtocolMessage.topic.length());
            out.writeBytes(PSProtocolMessage.topic.getBytes());
        }

        @Override
        public GiveMeYourReplicasMessage deserialize(ByteBuf in) throws UnknownHostException {
            short topicSize = in.readShort();
            byte[] topic = new byte[topicSize];
            in.readBytes(topic);


            return new GiveMeYourReplicasMessage( new String(topic));
        }

        @Override
        public int serializedSize(GiveMeYourReplicasMessage m) {
            return m.topic.length();
        }
    };

    public GiveMeYourReplicasMessage(String topic) {
        super(NOTIFICATION_ID);
        this.topic = topic;
    }


    public String getTopic() {
        return topic;
    }

    @Override
    public String toString() {
        return "PSProtocolMessage{" +
                "topic=" + topic + "," +
                 +
                '}';
    }
}
