package protocols.multipaxos;

import io.netty.buffer.ByteBuf;

import java.util.LinkedList;
import java.util.List;

public class OrderOperation {

    private String topic;
    private List<String> messages;

    public OrderOperation(String topic, List<String> messages) {
        this.topic = topic;
        this.messages = messages;
    }

    public static OrderOperation deserialize(ByteBuf in) {
        byte[] topicBytes = new byte[in.readInt()];
        in.readBytes(topicBytes);
        int size = in.readInt();
        List<String> messages = new LinkedList<>();

        for (int i = 0; i < size; i++) {
            byte[] messageBytes = new byte[in.readInt()];
            in.readBytes(messageBytes);
            messages.add(new String(messageBytes));
        }

        return new OrderOperation(new String(topicBytes), messages);
    }

    public String getTopic() {
        return topic;
    }

    public List<String> getMessages() {
        return messages;
    }

    public void serialize(ByteBuf out) {
        out.writeInt(this.topic.length());
        out.writeBytes(this.topic.getBytes());
        out.writeInt(this.messages.size());
        for (String message : messages) {
            out.writeInt(message.length());
            out.writeBytes(message.getBytes());
        }
    }

    public int serializedSize(){
        int size = 2 * Integer.BYTES + this.topic.length();
        for (String msg : this.messages) {
            size += msg.length() + Integer.BYTES;
        }

        return size;
    }
}
