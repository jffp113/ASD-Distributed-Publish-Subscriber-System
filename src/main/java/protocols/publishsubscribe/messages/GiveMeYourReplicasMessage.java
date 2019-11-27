package protocols.publishsubscribe.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;

import java.net.UnknownHostException;

public class GiveMeYourReplicasMessage extends ProtocolMessage {

    public final static short MSG_CODE = 102;
    private String topic;

    public static final ISerializer<GiveMeYourReplicasMessage> serializer = new ISerializer<GiveMeYourReplicasMessage>() {
        @Override
        public void serialize(GiveMeYourReplicasMessage PSProtocolMessage, ByteBuf out) {
            out.writeInt(PSProtocolMessage.topic.length());
            out.writeBytes(PSProtocolMessage.topic.getBytes());
        }

        @Override
        public GiveMeYourReplicasMessage deserialize(ByteBuf in) throws UnknownHostException {
            int topicSize = in.readInt();
            byte[] topic = new byte[topicSize];
            in.readBytes(topic);


            return new GiveMeYourReplicasMessage( new String(topic));
        }

        @Override
        public int serializedSize(GiveMeYourReplicasMessage m) {
            return m.topic.length() + Integer.BYTES;
        }
    };

    public GiveMeYourReplicasMessage(String topic) {
        super(MSG_CODE);
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
