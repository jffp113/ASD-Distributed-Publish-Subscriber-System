package protocols.dht.messagesTopics;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.Host;
import network.ISerializer;
import protocols.dht.messages.FindSuccessorRequestMessage;

import java.net.UnknownHostException;
import java.nio.charset.Charset;

public class ForwardSunscribeMessage extends ProtocolMessage {

    public static short MSG_CODE = 10341;
    private Host host;
    private String topic;
    private boolean isSubscribe;

    public ForwardSunscribeMessage(String topic,Host host, boolean isSubscribe) {
        super(MSG_CODE);
        this.topic = topic;
        this.host = host;
        this.isSubscribe =isSubscribe;
    }

    public String getTopic() {
        return topic;
    }

    public Host getHost() {
        return host;
    }

    public boolean isSubscribe() {
        return isSubscribe;
    }

    public static final ISerializer<ForwardSunscribeMessage> serializer = new ISerializer<ForwardSunscribeMessage>() {
        @Override
        public void serialize(ForwardSunscribeMessage m, ByteBuf out) {
            out.writeInt(m.topic.length());
            out.writeBytes(m.topic.getBytes());
            m.host.serialize(out);
            out.writeBoolean(m.isSubscribe);
        }

        @Override
        public ForwardSunscribeMessage deserialize(ByteBuf in) throws UnknownHostException {
            byte[] bytes = new byte[in.readInt()];
            in.readBytes(bytes);
            return new ForwardSunscribeMessage(new String(bytes), Host.deserialize(in),in.readBoolean());
        }

        @Override
        public int serializedSize(ForwardSunscribeMessage m) {
            return m.topic.getBytes().length + Integer.BYTES + 6;
        }
    };
}
