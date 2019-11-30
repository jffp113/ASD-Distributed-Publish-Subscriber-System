package protocols.publishsubscribe.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;

public class GetMessages extends ProtocolMessage {
    public final static short MSG_CODE = 10656;
    public static final ISerializer<GetMessages> serializer = new ISerializer<GetMessages>() {
        @Override
        public void serialize(GetMessages m, ByteBuf out) {
            out.writeInt(m.topic.length());
            out.writeBytes(m.topic.getBytes());
            out.writeInt(m.lowerBound);
            out.writeInt(m.upperBound);
        }

        @Override
        public GetMessages deserialize(ByteBuf in) {
            int topicSize = in.readInt();
            byte[] topic = new byte[topicSize];
            in.readBytes(topic);

            return new GetMessages(new String(topic), in.readInt(), in.readInt());
        }

        @Override
        public int serializedSize(GetMessages m) {
            return m.topic.length() + Integer.BYTES;
        }
    };
    private String topic;
    private int upperBound;
    private int lowerBound;

    public GetMessages(String topic, int lowerBound, int upperBound) {
        super(MSG_CODE);
        this.topic = topic;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
    }

    public String getTopic() {
        return topic;
    }

    @Override
    public String toString() {
        return "PSProtocolMessage{" +
                "topic=" + topic + "," + "upperBound=" + upperBound + ","
                + "LowerBound=" + lowerBound +
                "}";
    }

}
