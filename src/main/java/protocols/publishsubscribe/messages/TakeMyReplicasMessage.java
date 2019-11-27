package protocols.publishsubscribe.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.Host;
import network.ISerializer;

import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;

public class TakeMyReplicasMessage extends ProtocolMessage {

    public final static short MSG_CODE = 103;
    private String topic;
    private List<Host> replicas;

    public static final ISerializer<TakeMyReplicasMessage> serializer = new ISerializer<TakeMyReplicasMessage>() {
        @Override
        public void serialize(TakeMyReplicasMessage m, ByteBuf out) {
            out.writeInt(m.replicas.size());
            for(Host h : m.replicas){
                h.serialize(out);
            }
            out.writeShort(m.topic.length());
            out.writeBytes(m.topic.getBytes());
        }

        @Override
        public TakeMyReplicasMessage deserialize(ByteBuf in) throws UnknownHostException {
            List<Host> replicas = new LinkedList<>();

            int size = in.readInt();
            for(int i = 0 ; i < size ; i++){
                replicas.add(Host.deserialize(in));
            }

            short topicSize = in.readShort();
            byte[] topic = new byte[topicSize];
            in.readBytes(topic);

            return new TakeMyReplicasMessage(new String(topic),replicas);
        }

        @Override
        public int serializedSize(TakeMyReplicasMessage m) {
            return Integer.BYTES + m.replicas.size() * 6 + m.topic.length();
        }
    };

    public String getTopic() {
        return topic;
    }

    public List<Host> getReplicas() {
        return replicas;
    }

    public TakeMyReplicasMessage(String topic, List<Host> replicas) {
        super(MSG_CODE);
        this.topic = topic;
        this.replicas = replicas;
    }

    @Override
    public String toString() {
        return "PSProtocolMessage{" +
                "topic=" + topic + "," +
                 +
                '}';
    }
}
